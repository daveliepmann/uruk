(ns uruk.core
  "Marklogic XCC core functions: session management, querying, type
  conversion, transactions."
  (:require [clojure.set]
            [clojure.data.json :as json]
            [clojure.data.xml :as xml]
            [slingshot.slingshot :as sling])
  (:import [java.util.logging Logger] ;; XXX can this be omitted?
           [com.marklogic.xcc
            Session$TransactionMode
            RequestOptions
            ContentCreateOptions ContentPermission ContentCapability
            ContentSourceFactory ContentFactory
            DocumentFormat DocumentRepairLevel
            ValueFactory]
           [com.marklogic.xcc.types ValueType]
           java.net.URI))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;;; Type conversion
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn convert-number
  "Reads a number from a numeric Java object of a type from
  com.marklogic.xcc.types. Returns nil if not a number. Designed for
  robust number-handling while preventing read-string security
  issues. Regex from http://stackoverflow.com/a/12285023/706499. See
  https://docs.marklogic.com/javadoc/xcc/com/marklogic/xcc/types/package-summary.html"
  [obj]
  (let [s (.asString obj)]
    (when (re-find #"^-?\d+\.?\d*$" s)
      (clojure.edn/read-string s))))

(defn java-json->clj-json
  "Given a MarkLogic XCC JSON object, returns a Clojure representation
  of its JSON data."
  [java-json]
  (json/read-str (.toString (.asJsonNode java-json))))

(defn xdm-var->str
  "Returns a String representation of the given XDM variable"
  [xdm-var]
  (hash-map (.toString (.getName xdm-var))
            (.toString (.getValue xdm-var))))

(def types
  "Default mapping from MarkLogic XCC types (e.g. those that might be
  returned in a query's result set) to Clojure functions intended to
  convert to Clojure types. See
  https://docs.marklogic.com/javadoc/xcc/com/marklogic/xcc/types/package-summary.html"
  {"array-node()" java-json->clj-json ;; ArrayNode
   "boolean-node()" #(.asBoolean %)  ;; BooleanNode

   "cts:box" #(.asString %) ;; would be nice to convert box to seq of 4 numbers, unless that is disrupted by other element types
   "cts:circle" #(.asString %)
   "cts:point" #(.asString %)
   "cts:polygon" #(.asString %)

   "json:array" java-json->clj-json ;; JSArray
   "json:object" java-json->clj-json ;; JSObject

   ;; JsonItem TODO
   "null-node()" java-json->clj-json ;; NullNode
   "number-node()" convert-number ;; NumberNode
   "object-node()" java-json->clj-json ;; ObjectNode

   ;; XdmAtomic TODO
   "attribute()" #(.asString %) ;; XdmAttribute
   "binary()" #(.asBinaryData %) ;; XdmBinary
   "comment()" #(.asString %) ;; XdmComment
   "document-node()" #(.asString %) ;; XdmDocument
   "duration()" #(.toString %) ;; XdmDuration XXX may only ever come through as xs:duration
   "element()" #(.asString %) ;; XdmElement
   ;; XdmItem TODO
   "text()" #(.asString %) ;; XdmText
   "variable()" xdm-var->str ;; XdmVariable -- XXX unknown if the key used matches getValueType

   "xs:anyURI" str
   "xs:base64Binary" #(.asBinaryData %) ;; XSBase64Binary
   "xs:boolean" #(.asPrimitiveBoolean %)
   "xs:date" str
   "xs:dateTime" str
   "xs:dayTimeDuration" str
   "xs:decimal" convert-number
   "xs:double" convert-number
   "xs:duration" str
   "xs:float" convert-number
   ;; Maybe strip hyphens from all Gregorian date-parts?
   ;; or make conform to a particular date type?
   "xs:gDay" str
   "xs:gMonth" str
   "xs:gMonthDay" str
   "xs:gYear" str
   "xs:gYearMonth" str
   "xs:hexBinary" str ;; looks OK but test with real doc
   "xs:integer" convert-number
   "xs:QName" str
   "xs:string" str
   "xs:time" str
   "xs:untypedAtomic" str ;; NB: also referred to as "xdt:untypedAtomic" in type listing
   "xs:yearMonthDuration" str})

(defn result-type
  "Returns type string of the given query Result object. Currently
  assumes result is homogenous."
  [result]
  (.toString (.getValueType (first (.toArray result)))))

(defn convert-types
  "Return the result of applying type conversion to the given
  MarkLogic query result sequence. Default type mappings can be
  overridden (in part or in whole) with the optional parameter
  `type-mapping`, which should contain a transformation function keyed
  by an XCC type string. See `types` above."
  [result-sequence & [type-mapping]]
  ;; TODO throw informative exception if type not found in types
  (map (fn [item] (((merge types
                          (when (map? type-mapping)
                            type-mapping))
                   (.toString (.getValueType item))) item))
       (.toArray result-sequence)))


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;;; Helpers for sessions and requests
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(def transaction-modes
  "Mapping of keywords for valid Session transaction modes (per
  https://docs.marklogic.com/javadoc/xcc/com/marklogic/xcc/Session.TransactionMode.html)."
  {:auto Session$TransactionMode/AUTO
   :query Session$TransactionMode/QUERY
   :update Session$TransactionMode/UPDATE
   :update-auto-commit Session$TransactionMode/UPDATE_AUTO_COMMIT})

(def valid-request-options
  "Set of valid request options for Request objects. Can also be
  passed to Sessions as :default-request-options. See
  https://docs.marklogic.com/javadoc/xcc/com/marklogic/xcc/RequestOptions.html"
  #{:auto-retry-delay-millis
    :cache-result
    :default-xquery-version
    :effective-point-in-time
    :locale
    :max-auto-retry
    :query-language
    :request-name
    :request-time-limit
    :result-buffer-size
    :timeout-millis
    :timezone})

(defn request-options
  "Creates a Request Options object (to pass to a Request or a
  Session) out of the given options map. See `valid-request-options`
  for supported keywords."
  [options]
  (let [request (RequestOptions.)]
    (when-not (every? valid-request-options (keys options))
      ;; TODO switch to spec
      (throw (IllegalArgumentException. "Invalid request option. Keywords passed in `options` must be a subset of `valid-request-options`.")))
    (when-let [ardm (:auto-retry-delay-millis options)]
      (.setAutoRetryDelayMillis request ardm))
    (when (contains? options :cache-result)
      (.setCacheResult request (:cache-result options)))
    (when-let [dxv (:default-xquery-version options)]
      (.setDefaultXQueryVersion request dxv))
    ;; TODO A macro might be useful to replace the chain of when-lets with use-once variable names
    (when-let [epit (:effective-point-in-time options)]
      (.setEffectivePointInTime request (BigInteger. (str epit))))

    (when-let [locale (:locale options)]
      (.setLocale request locale)) ;; TODO enforce Locale object?
    (when-let [mar (:max-auto-retry options)]
      (.setMaxAutoRetry request (Integer. mar)))
    (when-let [ql (:query-language options)]
      (.setQueryLanguage request ql))
    (when-let [rn (:request-name options)]
      (.setRequestName request rn))

    (when-let [rtl (:request-time-limit options)]
      (.setRequestTimeLimit request rtl))
    (when-let [rbs (:result-buffer-size options)]
      (.setResultBufferSize request rbs))
    (when-let [tm (:timeout-millis options)]
      (.setTimeoutMillis request tm))
    (when-let [tz (:timezone options)] ;; TODO enforce type?
      (.setTimeZone request tz))
    ;; TODO check types of above
    ;; TODO test each
    request))

(defn- configure-session
  "Returns the given Session object, configured according to given
  options map. Assumes correct types are passed: map
  for :default-request-options, Logger object for :logger, Object
  for :user-object, keyword for :transaction-mode, integer
  for :transaction-timeout."
  [session options]
  (when-let [dro (:default-request-options options)]
    (.setDefaultRequestOptions session (request-options dro)))
  (when-let [lgr (:logger options)]
    (.setLogger session lgr))
  (when-let [uo (:user-object options)]
    (.setUserObject session uo))
  (when-let [tm (:transaction-mode options)]
    (.setTransactionMode session (transaction-modes tm)))
  (when-let [tt (:transaction-timeout options)]
    (.setTransactionTimeout session tt))
  session)


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;;; Session management
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn create-session
  "Create a Session for querying and transacting with. Parameter
  `db-info` describing database connection information must
  include :uri key, and may optionally include connection information
  for :content-base (database name), and/or :user and :password.

  See `newSession` methods at
  https://docs.marklogic.com/javadoc/xcc/com/marklogic/xcc/ContentSource.html
  for detail on allowed parameter arrangements.

  If optional `options` map is passed, the session is configured
  accordingly. See
  https://docs.marklogic.com/javadoc/xcc/com/marklogic/xcc/Session.html
  for valid options. (Note that request options are distinct from
  session options, though default request options *can* be set for the
  session.)"
  [db-info & [options]]
  (let [{:keys [uri user password content-base]} db-info
        cs (ContentSourceFactory/newContentSource (URI. (:uri db-info)))
        session (cond
                  (and (nil? content-base)
                       (or (nil? user)
                           (nil? password))) (.newSession cs)

                  (and (seq content-base)
                       (or (nil? user)
                           (nil? password))) (.newSession cs content-base)

                  (and (seq user)
                       (seq password)
                       (nil? content-base)) (.newSession cs user password)

                  (and (seq user)
                       (seq password)
                       (seq content-base)) (.newSession cs user password content-base))]
    (if (map? options)
      (configure-session session options)
      session)))


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;;; Queries
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn shape-results
  "Coerces the server's MarkLogic query response to the (possibly
  `nil`) `shape` that the client would like the response to take. By
  default, returns the unchanged server response.

  Recognized shapes include:
  :none - ignore the response
  :single - return just the first element of the response
  :single! - if the response is one element, return just that element;
  if not (i.e. if the response is more than one element) throw an
  error"
  [server-response shape]
  (case shape
    :none   nil
    ;; TODO burrow down to single result? or leave potential for seq results even with :single?
    :single (if (sequential? server-response)
              (first server-response)
              server-response)
    :single! (if (> (count server-response) 1)
               (throw (ex-info ":single! result shape specified, but result size is greater than 1."
                               {:result server-response}))
               (if (sequential? server-response)
                 (first server-response)
                 server-response))
    server-response))

(def variable-types
  "Mapping between Clojure keywords describing XDM Variable types and
  the Java representations of those types."
  {:array-node ValueType/ARRAY_NODE
   :attribute ValueType/ATTRIBUTE ;; FIXME Causes "Unhandled java.lang.InternalError | Unrecognized valueType: attribute()" if passed as variable :type
   :binary ValueType/BINARY
   :boolean-node ValueType/BOOLEAN_NODE
   :comment ValueType/COMMENT ;; FIXME if passed as variable :type, causes: com.marklogic.xcc.exceptions.XQueryException: XDMP-LEXVAL: xs:QName("comment()") -- Invalid lexical value "comment()" 
   :cts-box ValueType/CTS_BOX
   :cts-circle ValueType/CTS_CIRCLE
   :cts-point ValueType/CTS_POINT
   :cts-polygon ValueType/CTS_POLYGON

   :document ValueType/DOCUMENT ;; XXX document-node, really
   :element ValueType/ELEMENT
   :js-array ValueType/JS_ARRAY
   :js-object ValueType/JS_OBJECT
   :node ValueType/NODE
   :null-node ValueType/NULL_NODE
   :number-node ValueType/NUMBER_NODE
   :object-node ValueType/OBJECT_NODE
   :processing-instruction ValueType/PROCESSING_INSTRUCTION
   :sequence ValueType/SEQUENCE
   :text ValueType/TEXT ;; FIXME causes XDMP-LEXVAL

   :xs-any-uri ValueType/XS_ANY_URI
   :xs-base64-binary ValueType/XS_BASE64_BINARY
   :xs-boolean ValueType/XS_BOOLEAN
   :xs-date ValueType/XS_DATE
   :xs-date-time ValueType/XS_DATE_TIME
   :xs-day-time-duration ValueType/XS_DAY_TIME_DURATION
   :xs-decimal ValueType/XS_DECIMAL
   :xs-double ValueType/XS_DOUBLE
   :xs-duration ValueType/XS_DURATION
   :xs-float ValueType/XS_FLOAT

   :xs-gday ValueType/XS_GDAY
   :xs-gmonth ValueType/XS_GMONTH
   :xs-gmonth-day ValueType/XS_GMONTH_DAY
   :xs-gyear ValueType/XS_GYEAR
   :xs-gyear-month ValueType/XS_GYEAR_MONTH

   :xs-hex-binary ValueType/XS_HEX_BINARY
   :xs-integer ValueType/XS_INTEGER
   :xs-qname ValueType/XS_QNAME
   :xs-string ValueType/XS_STRING
   :xs-time ValueType/XS_TIME
   :xs-untyped-atomic ValueType/XS_UNTYPED_ATOMIC
   :xs-year-month-duration ValueType/XS_YEAR_MONTH_DURATION

   ;; default:
   nil ValueType/XS_STRING})

(defn ->xml-str
  "Assumes that its input is valid XML in some format, returning that
  XML in its String representation. Accepts String, hiccup-style
  vectors, and clojure.data.xml.Element."
  [xml]
  (cond
    (string? xml) xml
    (vector? xml) (xml/emit-str (xml/sexp-as-element xml))
    (instance? clojure.data.xml.Element xml) (xml/emit-str xml)))

(defn wrap-val
  "Given a Clojure value, returns a value in a type appropriate for a
  MarkLogic XdmVariable."
  [clj-val type]
  (case type
    :array-node (ValueFactory/newArrayNode (json/write-str clj-val))
    :boolean-node (-> (com.fasterxml.jackson.databind.node.JsonNodeFactory/instance)
                      (.booleanNode clj-val)
                      ValueFactory/newBooleanNode)
    :attribute clj-val ;; doesn't seem to be implemented by ValueFactory: "java.lang.InternalError Unrecognized valueType: attribute()"
    :binary clj-val ;; TODO
    :comment clj-val ;; TODO, also really :comment-node

    :cts-box (str clj-val) ;; 4-element sequence or String
    :cts-circle (let [[radius [latitude longitude]] clj-val]
                  (.asString (ValueFactory/newCtsCircle (str radius)
                                                        (ValueFactory/newCtsPoint (str latitude)
                                                                                  (str longitude)))))
    :cts-point (str (first clj-val) ","
                    (second clj-val)) ;; Expects a 2-number sequence
    :cts-polygon (let [vertices clj-val]
                   ;; Expects a sequence of 2-element sequences, each
                   ;; describing successive vertices (points) of the
                   ;; polygon
                   (.asString (ValueFactory/newCtsPolygon
                               (mapv (fn [latitude longitude]
                                       (ValueFactory/newCtsPoint (str latitude)
                                                                 (str longitude)))
                                     vertices))))

    :document (->xml-str clj-val) ;; :document-node, really, but we're following ValueType
    :element (->xml-str clj-val)
    :js-array (ValueFactory/newJSArray (json/write-str clj-val))
    :js-object (ValueFactory/newJSObject (json/write-str clj-val))
    :node clj-val
    :null-node nil
    :number-node (str clj-val)
    :object-node (ValueFactory/newObjectNode (json/write-str clj-val))
    :processing-instruction clj-val ;; TODO
    :sequence clj-val ;; XXX WONTFIX -- see GitHub issue #8: "com.marklogic.xcc.exceptions.UnimplementedFeatureException - Setting variables that are sequences is not supported"
    :text clj-val ;; TODO, also really :text-node

    :xs-any-uri clj-val ;; TODO
    :xs-base64-binary clj-val ;; TODO
    :xs-boolean clj-val
    :xs-date clj-val ;; TODO
    :xs-date-time clj-val ;; TODO
    :xs-day-time-duration clj-val ;; TODO
    :xs-decimal clj-val
    :xs-double clj-val
    :xs-duration clj-val
    :xs-float clj-val

    :xs-gday clj-val ;; TODO
    :xs-gmonth clj-val ;; TODO
    :xs-gmonth-day clj-val ;; TODO
    :xs-gyear clj-val ;; TODO
    :xs-gyear-month clj-val ;; TODO

    :xs-hex-binary clj-val ;; TODO
    :xs-integer clj-val
    :xs-qname clj-val ;; TODO
    :xs-string (str clj-val)
    :xs-time clj-val ;; TODO
    :xs-untyped-atomic clj-val ;; TODO
    :xs-year-month-duration clj-val ;; TODO

    clj-val))

(defn- request-obj
  "Build a Request object using the given `request-factory` builder,
  request `options`, and bindings for XQuery external `variables`."
  [request-factory options variables]
  (reduce-kv (fn [acc vname vval]
               (if (string? vval)
                 (.setNewVariable acc (name vname)
                                  ValueType/XS_STRING vval)
                 (let [{:keys [namespace type value as-is?]} vval]
                   (if (string? namespace)
                     (.setNewVariable acc (name vname) namespace
                                      (variable-types type) value)
                     (.setNewVariable acc (name vname)
                                      (variable-types type)
                                      (if as-is?
                                        value
                                        (wrap-val value type))))))
               acc)
             (doto request-factory (.setOptions (request-options options)))
             variables))

(defn submit-request
  "Construct, submit, and return raw results of request for the given
  `session` using `request-factory` and `query`. Modify it
  with (possibly empty) `options` and `variables` maps. Applies type
  conversion to response according to defaults and `types`. Variables
  may be passed as a map of Strings or with String names corresponding
  to maps describing the variable using mandatory key `:value` and
  optional keys `:namespace` and `:type`.`"
  [request-factory session query options variables types shape]
  (let [req (sling/try+ (.submitRequest session
                                        (request-obj request-factory options variables))
                        ;; RequestException and its subclass
                        ;; XQueryException have overridden toString,
                        ;; so only those need to be re-thrown with
                        ;; that info in `message`, otherwise it is
                        ;; hidden by most Clojure middleware (probably
                        ;; nrepl, possibly leiningen and cider)
                        (catch com.marklogic.xcc.exceptions.XQueryException e
                          (sling/throw+ (doto (com.marklogic.xcc.exceptions.XQueryException.
                                               ;; See https://github.com/marklogic/xcc-java/blob/master/com/marklogic/xcc/exceptions/XQueryException.java#L63
                                               (.getRequest e)
                                               (.getCode e)
                                               (.getW3CCode e)
                                               (.getXQueryVersion e)
                                               (.toString e)
                                               (.getFormatString e)
                                               "" ;; FIXME expr - "The expression that caused the exception, if applicable"
                                               (.isRetryable e)
                                               (.getData e)
                                               (.getStack e))
                                          (.setStackTrace (:stack-trace &throw-context)))))
                        ;; TODO It could be useful to re-throw other
                        ;; specific sub-classes of RequestException.
                        (catch com.marklogic.xcc.exceptions.RequestException e
                          (sling/throw+ (doto (com.marklogic.xcc.exceptions.RequestException.
                                               (.toString e)
                                               (.getRequest e))
                                          (.setStackTrace (:stack-trace &throw-context))))))]
    (shape-results (cond (= :raw types) req
                         :else          (convert-types req types))
                   shape)))

(defn execute-xquery
  "Execute the given xquery query as a request to the database
  connection defined by the given session. Takes an optional
  configuration map describing request `options` and `variables`,
  desired `shape` of the result, and overrides of default type
  conversion in `types`.

  Options passed must be in `valid-request-options` and conform to
  `request-options`.

  Variables may be passed as a map of Strings or with String names
  corresponding to maps describing the variable using mandatory key
  `:value` and optional keys `:namespace` and `:type`.`

  The shape of results is coerced using `shape-results` if the
  `:shape` key is passed. For example, a value of `:single` will
  return only the first value.

  Type conversion overrides must be a map using keys present in
  `uruk.core/types` and conform to use in `convert-types`, that is,
  including values which are a function of one variable."
  ([session query]
   (execute-xquery session query {}))
  ([session query {:keys [options variables types shape]}]
   (submit-request (.newAdhocQuery session query) session query
                   options variables types shape)))

(defn execute-module
  "Execute the named module as a request to the database connection
  defined by the given session. Takes an optional configuration map
  describing request `options` and `variables`, desired `shape` of the
  result, and overrides of default type conversion in `types`.

  Options passed must be in `valid-request-options` and conform to
  `request-options`.

  Variables may be passed as a map of Strings or with String names
  corresponding to maps describing the variable using mandatory key
  `:value` and optional keys `:namespace` and `:type`.`

  The shape of results is coerced using `shape-results` if the
  `:shape` key is passed. For example, a value of `:single` will
  return only the first value.

  Type conversion overrides must be a map using keys present in
  `uruk.core/types` and conform to use in `convert-types`, that is,
  including values which are a function of one variable."
  ([session module]
   (execute-module session module {}))
  ([session module {:keys [options variables types shape]}]
   (submit-request (.newModuleInvoke session module) session module
                   options variables types shape)))

;;;;;

(def valid-content-creation-options
  "Set of valid creation options for Content objects. See
  https://docs.marklogic.com/javadoc/xcc/com/marklogic/xcc/ContentCreateOptions.html"
  #{:buffer-size
    :collections
    :encoding
    :format
    ;; The following keys correspond to convenience methods in Java
    ;; that I don't currently find useful or idiomatic in Clojure:
    ;; :format-binary :format-json :format-text :format-xml

    :graph
    :language
    :locale
    :namespace
    :permissions
    :placement-keys
    :quality
    :repair-level
    :resolve-buffer-size
    :resolve-entities
    :temporal-collection})

(def doc-format
  {:xml    DocumentFormat/XML
   :json   DocumentFormat/JSON
   :text   DocumentFormat/TEXT
   :none   DocumentFormat/NONE
   :binary DocumentFormat/BINARY})

(def repair-level
  {:default DocumentRepairLevel/DEFAULT
   :full    DocumentRepairLevel/FULL
   :none    DocumentRepairLevel/NONE})

(defn content-creation-options
  "Creates a ContentCreateOptions object (to pass to a ContentFactory
  newContent call) out of the given options map. See
  `valid-content-creation-options` for supported keywords."
  [options]
  (let [cco (ContentCreateOptions.)]
    (when-not (every? valid-content-creation-options (keys options))
      ;; TODO switch to spec
      (throw (IllegalArgumentException. "Invalid content creation option. Keywords passed in `options` must be a subset of `valid-content-creation-options`.")))
    (when-let [bs (:buffer-size options)]
      (.setBufferSize cco bs))
    (when-let [c (:collections options)]
      (.setCollections cco (into-array String c)))
    (when-let [e (:encoding options)]
      (.setEncoding cco e))
    (when-let [f (:format options)]
      (.setFormat cco (doc-format f)))

    (when-let [g (:graph options)]
      (.setGraph cco g))
    (when-let [lang (:language options)]
      (.setLanguage cco lang))
    (when-let [locale (:locale options)]
      (.setLocale cco locale))
    (when-let [n (:namespace options)]
      (.setNamespace cco n))
    (when-let [perms (:permissions options)]
      (.setPermissions cco (into-array ContentPermission
                                       (reduce (fn [permissions permission]
                                                 (conj permissions
                                                       (ContentPermission.
                                                        (case (val (first permission))
                                                          :execute ContentCapability/EXECUTE
                                                          :insert  ContentCapability/INSERT
                                                          :read    ContentCapability/READ
                                                          :update  ContentCapability/UPDATE)
                                                        (key (first permission)))))
                                               []
                                               perms))))

    (when-let [pk (:placement-keys options)]
      (.setPlaceKeys cco pk)) ;; FIXME bigint and long array casts?
    (when-let [q (:quality options)]
      (.setQuality cco q))
    (when-let [rl (:repair-level options)]
      (.setRepairLevel cco (repair-level rl)))
    (when-let [rbs (:resolve-buffer-size options)]
      (.setResolveBufferSize cco rbs))
    (when-let [re (:resolve-entities options)]
      (.setResolveEntities cco re))
    (when-let [tc (:temporal-collection options)]
      (.setTemporalCollection cco tc))
    cco))

(defn describe-content-creation-options
  [opts]
  {:buffer-size (.getBufferSize opts)
   :collections (mapv #(.toString %) (.getCollections opts))
   :encoding (.getEncoding opts)
   :format ((clojure.set/map-invert doc-format) (.getFormat opts))
   :graph (.getGraph opts)
   :language (.getLanguage opts)
   :namespace (.getNamespace opts)
   :permissions (mapv #(hash-map (.getRole %)
                                 (keyword (.toString (.getCapability %))))
                      (.getPermissions opts))
   :quality (.getQuality opts)
   :repair-level ((clojure.set/map-invert repair-level) (.getRepairLevel opts))
   :resolve-buffer-size (.getResolveBufferSize opts)
   :resolve-entities (.getResolveEntities opts)
   :temporal-collection (.getTemporalCollection opts)})

(defn element->content
  "Given a clojure.data.xml.Element, returns a MarkLogic XCC Content
  object suitable for inserting to a database. Optionally takes a map
  of content creation options per `content-creation-options`. Defaults
  to XML-formatted documents.

  See https://docs.marklogic.com/javadoc/xcc/com/marklogic/xcc/Content.html
  and https://docs.marklogic.com/javadoc/xcc/com/marklogic/xcc/ContentFactory.html"
  ([uri element]
   (element->content uri element {:format :xml}))
  ([uri element options]
   (ContentFactory/newContent uri (xml/emit-str element)
                              (content-creation-options (merge {:format :xml}
                                                               options)))))

(defn insert-element
  "Inserts the given clojure.data.xml.Element `element` at the given
  `uri` to the database/content-base according determined by the
  current `session`. Optionally takes a map of content creation
  options per `content-creation-options`.

  See https://docs.marklogic.com/javadoc/xcc/com/marklogic/xcc/Session.html#insertContent(com.marklogic.xcc.Content)"
  ([session uri element]
   (.insertContent session (element->content uri element)))
  ([session uri element options]
   (.insertContent session (element->content uri element options))))

;; TODO? https://docs.marklogic.com/javadoc/xcc/com/marklogic/xcc/Session.html#insertContent(com.marklogic.xcc.Content[])
;; TODO? https://docs.marklogic.com/javadoc/xcc/com/marklogic/xcc/Session.html#insertContentCollectErrors(com.marklogic.xcc.Content[])

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;;; Transactions
;;;;
;;;; Users must manage their own transactions, either from within
;;;; XQuery or programmatically. If working programmatically, wrapping
;;;; requests in `with-open` is strongly recommended. One may use
;;;; these functions within `with-open` after setting the transaction
;;;; mode to `:update` or `:query` on the session via the
;;;; `:transaction-mode` option.
;;;;
;;;; See https://docs.marklogic.com/guide/xcc/concepts#id_23310
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn commit
  "Commit `session` when current queries successfully finish."
  [session]
  (.commit session))

(defn rollback
  "Rollback a multi-statement transaction to reset any un-committed
  transactions that have already occured in that transaction; for
  example, delete any created items, restore any deleted items, revert
  back any edits, etc."
  [session]
  (.rollback session))
