(ns uruk.core
  "Marklogic XCC core functions: session management, querying, type
  conversion, transactions."
  (:require [clojure.data.json :as json]
            [clojure.data.xml :as xml])
  (:import [java.util.logging Logger]
           [com.marklogic.xcc
            Session$TransactionMode
            RequestOptions
            ContentCreateOptions ContentPermission ContentCapability
            ContentSourceFactory ContentFactory
            DocumentFormat DocumentRepairLevel]
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
   "variable()" #(hash-map (.toString (.getName %))
                           (.asString (.getValue %))) ;; XdmVariable -- XXX unknown if the key used matches getValueType

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
  (let [result (map (fn [item] (((merge types
                                       (when (map? type-mapping)
                                         type-mapping))
                                (.toString (.getValueType item))) item))
                    (.toArray result-sequence))]
    (if (= 1 (count result))
      (first result)
      result)))


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

(defn- request-options
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
;;;; Working with sessions
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn submit-request
  "Construct, submit, and return raw results of request for the given
  `session` using `request-factory` and `query`. Modify it
  with (possibly empty) `options` and `variables` maps. Applies type
  conversion according to defaults and `types`."
  [request-factory session query options variables types]
  (let [ro      (request-options options)
        request (reduce-kv (fn [acc vname vval]
                             (.setNewVariable acc (name vname)
                                              ValueType/XS_STRING (str vval))
                             acc)
                           (doto request-factory (.setOptions ro))
                           options)]
    (cond (= :raw types) (.submitRequest session request)
          :else (convert-types (.submitRequest session request) types))))

(defn execute-xquery
  "Execute the given xquery query as a request to the database
  connection defined by the given session. Apply request options or
  variables if given. Applies default type conversion, overridden by
  `types` map if given."
  [session query & {:keys [options variables types]}]
  (submit-request (.newAdhocQuery session query) session query options variables types))

(defn execute-module
  "Execute the named module as a request to the database connection
  defined by the given session. Apply request options or variables if
  given. Applies default type conversion, overridden by `types` map if
  given."
  [session module & {:keys [options variables types]}]
  (submit-request (.newModuleInvoke session module) session module options variables types))

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
                                                 (conj permissions (ContentPermission. (case (val (first permission))
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
   :collections (map #(.toString %) (.getCollections opts))
   :encoding (.getEncoding opts)
   :format ((clojure.set/map-invert doc-format) (.getFormat opts))
   :graph (.getGraph opts)
   :language (.getLanguage opts)
   :namespace (.getNamespace opts)
   :permissions (map #(hash-map (.getRole %) (.toString (.getCapability %))) (.getPermissions opts)) 
   :quality (.getQuality opts)
   :repair-level ((clojure.set/map-invert repair-level) (.getRepairLevel opts))
   :resolve-buffer-size (.getResolveBufferSize opts)
   :resolve-entities (.getResolveEntities opts)
   :temporal-collection (.getTemporalCollection opts)})

(defn element->content
  "Given a clojure.data.xml.Element, returns a MarkLogic XCC Content
  object suitable for inserting to a database. Optionally takes a map
  of content creation options per `content-creation-options`.

  See https://docs.marklogic.com/javadoc/xcc/com/marklogic/xcc/Content.html
  and https://docs.marklogic.com/javadoc/xcc/com/marklogic/xcc/ContentFactory.html"
  ([uri element]
   (element->content uri element {:format :xml}))
  ([uri element options]
   (ContentFactory/newContent uri (xml/emit-str element)
                              (content-creation-options options))))

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
