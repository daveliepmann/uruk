(ns uruk.core
  "Marklogic XCC core functions: session management, querying, type
  conversion, transactions."
  (:require clojure.set
            clojure.edn
            [clojure.data.json :as json]
            [clojure.data.xml :as xml]
            [clojure.data.xml.tree :as tree]
            [slingshot.slingshot :as sling])
  (:import [java.util.logging Logger]
           java.net.URI
           [com.marklogic.xcc
            Session$TransactionMode ;; XXX DEPRECATED, will be removed in future version
            Session$Update
            RequestOptions SecurityOptions
            ContentCreateOptions ContentPermission ContentCapability
            ContentSource ContentSourceFactory ContentFactory
            DocumentFormat DocumentRepairLevel
            ValueFactory Version]
           [com.marklogic.xcc.types ValueType]
           [com.marklogic.xcc.spi ConnectionProvider]))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;;; Enumerations and classes.
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(def version
  "XCC release number. Auto-generated.
  See https://docs.marklogic.com/javadoc/xcc/com/marklogic/xcc/Version.html"
  {:string (Version/getVersionString)
   :version-major (Version/getVersionMajor)
   :version-minor (Version/getVersionMinor)
   :version-patch (Version/getVersionPatch)})

(def ->doc-format
  "Mapping of keywords to allowed document formats. Used at insertion time.
  See https://docs.marklogic.com/javadoc/xcc/com/marklogic/xcc/DocumentFormat.html"
  {:xml    DocumentFormat/XML
   :json   DocumentFormat/JSON
   :text   DocumentFormat/TEXT
   :none   DocumentFormat/NONE
   :binary DocumentFormat/BINARY})

(def ->doc-repair-level
  "Mapping of keywords to document repair levels. Used at insertion time.
  See https://docs.marklogic.com/javadoc/xcc/com/marklogic/xcc/DocumentRepairLevel.html"
  {:default DocumentRepairLevel/DEFAULT
   :full    DocumentRepairLevel/FULL
   :none    DocumentRepairLevel/NONE})

(def ->content-capability
  "Mapping of keywords to content permission capability values.
  See https://docs.marklogic.com/javadoc/xcc/com/marklogic/xcc/ContentCapability.html"
  {:execute ContentCapability/EXECUTE
   :insert  ContentCapability/INSERT
   :read    ContentCapability/READ
   :update  ContentCapability/UPDATE})

(def ->transaction-mode
  "DEPRECATED - see https://docs.marklogic.com/guide/relnotes/chap5#id_91389
  Mapping of keywords to valid Session transaction modes. See
  https://docs.marklogic.com/javadoc/xcc/com/marklogic/xcc/Session.TransactionMode.html"
  ^{:deprecated "9.0-2"}
  {:auto               Session$TransactionMode/AUTO ;; XXX DEPRECATED
   :query              Session$TransactionMode/QUERY ;; XXX DEPRECATED
   :update             Session$TransactionMode/UPDATE ;; XXX DEPRECATED
   :update-auto-commit Session$TransactionMode/UPDATE_AUTO_COMMIT ;; XXX DEPRECATED
   })

(def ->update-mode
  "Mapping of keywords _and_ booleans to valid Session update modes. See
  https://docs.marklogic.com/javadoc/xcc/com/marklogic/xcc/Session.Update.html"
  ;; Please be aware of the discrepancy between MarkLogic XCC naming (Enum Session.Update) and Uruk's (update-mode), in order to avoid conflicting with clojure.core's `update` function.
  {false  Session$Update/FALSE
   :false Session$Update/FALSE
   :auto  Session$Update/AUTO
   true   Session$Update/TRUE
   :true  Session$Update/TRUE})

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;;; Type conversion
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn java->num
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
  "Given a MarkLogic XCC JSON object, returns a Clojure representation of its JSON data."
  [java-json]
  (json/read-str (.toString (.asJsonNode java-json))))

(defn xdm-var->str
  "Returns a String representation of the given XDM variable"
  [xdm-var]
  (hash-map (.toString (.getName xdm-var))
            (.toString (.getValue xdm-var))))

(defn ->xml-str
  "Assumes that its input is valid XML in some format, returning that
  XML in its String representation. Accepts String, hiccup-style
  vectors, and clojure.data.xml.Element."
  [xml]
  (cond
    (string? xml) xml
    (vector? xml) (xml/emit-str (xml/sexp-as-element xml))
    (instance? clojure.data.xml.node.Element xml) (xml/emit-str xml)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;;; XCC type table.
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(def xcc-types
  "Lookup table for XCC type information.

  Includes all Clojure-relevant info about XCC types (and XML schema
  types relevant to MarkLogic XCC). Provides functions for conversion
  to and from XCC types and XdmVariable-suitable objects from Clojure.

  Values are keyed by keyword describing the XCC type. Each value is
  organized as follows:

  `:ml->clj` maps to the function used to convert from MarkLogic to
  Clojure

  `:clj->xdm` maps to the function used to convert from Clojure to a
  value appropriate for use in an XDM variable

  `:xml-name` maps to the string used to refer to this type in
  XQuery (its XML schema name)

  `:xcc-type` maps to the corresponding
  com.marklogic.xcc.types.ValueType field"
  {:attribute {;; FIXME Causes "Unhandled java.lang.InternalError | Unrecognized valueType: attribute()" if passed as variable :type
               :ml->clj #(.asString %)
               :clj->xdm identity ;; doesn't seem to be implemented by ValueFactory: "java.lang.InternalError Unrecognized valueType: attribute()"
               :xml-name "attribute()"
               :xcc-type ValueType/ATTRIBUTE}
   :binary {:ml->clj #(.asBinaryData %)
            :clj->xdm identity ;; TODO
            :xml-name "binary()"
            :xcc-type ValueType/BINARY}

   :array-node {:ml->clj java-json->clj-json
                :clj->xdm #(ValueFactory/newArrayNode (json/write-str %))
                :xml-name "array-node()"
                :xcc-type ValueType/ARRAY_NODE}
   :boolean-node {:ml->clj #(.asBoolean %)
                  :clj->xdm #(-> (com.fasterxml.jackson.databind.node.JsonNodeFactory/instance)
                                 (.booleanNode %)
                                 ValueFactory/newBooleanNode)
                  :xml-name "boolean-node()"
                  :xcc-type ValueType/BOOLEAN_NODE}
   :comment {:ml->clj #(.asString %)
             :clj->xdm identity ;; TODO, also really :comment-node
             :xml-name "comment()"
             :xcc-type ValueType/COMMENT} ;; FIXME if passed as variable :type, causes: com.marklogic.xcc.exceptions.XQueryException: XDMP-LEXVAL: xs:QName("comment()") -- Invalid lexical value "comment()"

   :cts-box {:ml->clj #(.asString %) ;; would be nice to convert box to seq of 4 numbers, unless that is disrupted by other element types
             :clj->xdm str
             :xml-name "cts:box"
             :xcc-type ValueType/CTS_BOX}
   :cts-circle {:ml->clj #(.asString %)
                :clj->xdm (fn [[radius [latitude longitude]]]
                            ;; Expects a 2-element vector containing a
                            ;; number followed by a 2-element vector
                            ;; of numbers
                            (.asString (ValueFactory/newCtsCircle
                                        (str radius)
                                        (ValueFactory/newCtsPoint (str latitude)
                                                                  (str longitude)))))
                :xml-name "cts:circle"
                :xcc-type ValueType/CTS_CIRCLE}

   :cts-point {:ml->clj #(.asString %)
               :clj->xdm #(let [[latitude longitude] %]
                            ;; Expects a 2-number sequence
                            (.asString (ValueFactory/newCtsPoint (str latitude)
                                                                 (str longitude))))
               :xml-name "cts:point"
               :xcc-type ValueType/CTS_POINT}
   :cts-polygon {:ml->clj #(.asString %)
                 :clj->xdm (fn [vertices]
                             ;; Expects a sequence of 2-element sequences, each
                             ;; describing successive vertices (points) of the
                             ;; polygon
                             (.asString (ValueFactory/newCtsPolygon
                                         (mapv (fn [[latitude longitude]]
                                                 (ValueFactory/newCtsPoint (str latitude)
                                                                           (str longitude)))
                                               vertices))))
                 :xml-name "cts:polygon"
                 :xcc-type ValueType/CTS_POLYGON}

   :document {:ml->clj #(.asString %) ;; XdmDocument
              :clj->xdm ->xml-str
              :xml-name "document-node()" ;; XXX key should be :document-node, really
              :xcc-type ValueType/DOCUMENT}
   :element {:ml->clj  #(.asString %)
             :clj->xdm ->xml-str
             :xml-name "element()"
             :xcc-type ValueType/ELEMENT}

   :js-array {:ml->clj java-json->clj-json
              :clj->xdm #(json/write-str %)
              :xml-name "json:array"
              :xcc-type ValueType/JS_ARRAY}
   :js-object {:ml->clj java-json->clj-json
               :clj->xdm #(json/write-str %)
               :xml-name "json:object"
               :xcc-type ValueType/JS_OBJECT}

   :node {:ml->clj str
          :clj->xdm identity
          :xml-name nil ;; TODO XXX ???
          :xcc-type ValueType/NODE}
   :null-node {:ml->clj java-json->clj-json
               :clj->xdm #(ValueFactory/newNullNode %)
               :xml-name "null-node()"
               :xcc-type ValueType/NULL_NODE}
   :number-node {:ml->clj java->num
                 :clj->xdm str
                 :xml-name "number-node()"
                 :xcc-type ValueType/NUMBER_NODE}
   :object-node {:ml->clj java-json->clj-json
                 :clj->xdm #(ValueFactory/newObjectNode (json/write-str %))
                 :xml-name "object-node()"
                 :xcc-type ValueType/OBJECT_NODE}

   :processing-instruction {:ml->clj str
                            :clj->xdm str ;; TODO test?
                            :xml-name nil ;; XXX ???
                            :xcc-type ValueType/PROCESSING_INSTRUCTION}
   :sequence {:ml->clj str
              ;; :clj->xdm
              ;; XXX WONTFIX -- see GitHub issue #8: "com.marklogic.xcc.exceptions.UnimplementedFeatureException - Setting variables that are sequences is not supported"
              ;; https://github.com/marklogic/xcc-java/blob/master/com/marklogic/xcc/impl/RequestImpl.java#L92
              :xml-name nil ;; XXX ???
              :xcc-type ValueType/SEQUENCE}
   :text {;; really :text-node
          :ml->clj #(.asString %)
          :clj->xdm (fn [s] (ValueFactory/newObjectNode s)) ;; FIXME all conversion fns cause XDMP-LEXVAL
          :xml-name "text()"
          :xcc-type ValueType/TEXT}
   :variable {;; XdmVariable. Unknown if the key used matches getValueType.
              :ml->clj xdm-var->str
              :clj->xdm identity  ;; FIXME
              :xml-name "variable()"
              ;; FIXME
              :xcc-type nil}

   :xs-any-uri {:ml->clj str
                :clj->xdm identity ;; TODO
                :xml-name "xs:anyURI"
                :xcc-type ValueType/XS_ANY_URI}
   :xs-base64-binary {:ml->clj #(.asBinaryData %)
                      :clj->xdm identity ;; TODO
                      :xml-name "xs:base64Binary"
                      :xcc-type ValueType/XS_BASE64_BINARY}
   :xs-boolean {:ml->clj #(.asPrimitiveBoolean %)
                :clj->xdm identity
                :xml-name "xs:boolean"
                :xcc-type ValueType/XS_BOOLEAN}

   :xs-time {:ml->clj str
             :clj->xdm identity ;; TODO
             :xml-name "xs:time"
             :xcc-type ValueType/XS_TIME}
   :xs-date {:ml->clj str
             :clj->xdm identity ;; TODO
             :xml-name "xs:date"
             :xcc-type ValueType/XS_DATE}
   :xs-date-time {:ml->clj str
                  :clj->xdm identity ;; TODO
                  :xml-name "xs:dateTime"
                  :xcc-type ValueType/XS_DATE_TIME}

   ;; Numbers
   :xs-integer {:ml->clj java->num
                :clj->xdm identity
                :xml-name "xs:integer"
                :xcc-type ValueType/XS_INTEGER}
   :xs-decimal {:ml->clj java->num
                :clj->xdm identity
                :xml-name "xs:decimal"
                :xcc-type ValueType/XS_DECIMAL}
   :xs-double {:ml->clj java->num
               :clj->xdm identity
               :xml-name "xs:double"
               :xcc-type ValueType/XS_DOUBLE}
   :xs-float {:ml->clj java->num
              :clj->xdm identity
              :xml-name "xs:float"
              :xcc-type ValueType/XS_FLOAT}

   ;; Durations
   :duration {;; XdmDuration should only ever come through as
              ;; xs:duration; this is a purely Java construct.
              :ml->clj #(.toString %)
              :clj->xdm identity ;; TODO test
              :xml-name "duration()"
              :xcc-type nil}
   :xs-duration {:ml->clj str
                 :clj->xdm identity
                 :xml-name "xs:duration"
                 :xcc-type ValueType/XS_DURATION}
   :xs-day-time-duration {:ml->clj str
                          :clj->xdm identity ;; TODO
                          :xml-name "xs:dayTimeDuration"
                          ;; UML diagram says "XDT_DAY_TIME_DURATION" but that doesn't resolve
                          :xcc-type ValueType/XS_DAY_TIME_DURATION}
   :xs-year-month-duration {:ml->clj str
                            :clj->xdm identity ;; TODO
                            :xml-name "xs:yearMonthDuration"
                            ;; UML diagram says "XDT_YEAR_MONTH_DURATION" but that doesn't resolve
                            :xcc-type ValueType/XS_YEAR_MONTH_DURATION}

   ;; Gregorian dates/times
   :xs-gday {:ml->clj str
             :clj->xdm identity ;; TODO
             :xml-name "xs:gDay"
             :xcc-type ValueType/XS_GDAY}
   :xs-gmonth {:ml->clj str
               :clj->xdm identity ;; TODO
               :xml-name "xs:gMonth"
               :xcc-type ValueType/XS_GMONTH}
   :xs-gmonth-day {:ml->clj str
                   :clj->xdm identity ;; TODO
                   :xml-name "xs:gMonthDay"
                   :xcc-type ValueType/XS_GMONTH_DAY}
   :xs-gyear {:ml->clj str
              :clj->xdm identity ;; TODO
              :xml-name "xs:gYear"
              :xcc-type ValueType/XS_GYEAR}
   :xs-gyear-month {:ml->clj str
                    :clj->xdm identity ;; TODO
                    :xml-name "xs:gYearMonth"
                    :xcc-type ValueType/XS_GYEAR_MONTH}

   :xs-hex-binary {:ml->clj str ;; looks OK but needs to be tested with real doc
                   :clj->xdm identity ;; TODO
                   :xml-name "xs:hexBinary"
                   :xcc-type ValueType/XS_HEX_BINARY}
   :xs-qname {:ml->clj str
              :clj->xdm identity ;; TODO
              :xml-name "xs:QName"
              :xcc-type ValueType/XS_QNAME}
   :xs-string {:ml->clj str
               :clj->xdm str
               :xml-name "xs:string"
               :xcc-type ValueType/XS_STRING}
   :xs-untyped-atomic {:ml->clj str
                       :clj->xdm identity ;; TODO
                       ;; NB: also referred to as "xdt:untypedAtomic" in type listing
                       :xml-name "xs:untypedAtomic"
                       ;; UML diagram says "XDT_UNTYPED_ATOMIC" but that doesn't resolve
                       :xcc-type ValueType/XS_UNTYPED_ATOMIC}

   ;; Default:
   nil {:ml->clj str
        :clj->xdm identity ;; TODO
        :xml-name nil
        :xcc-type ValueType/XS_STRING}})

(def xml-type-str->conv-fn
  "Default mapping from XML Schema type strings to Clojure functions
  that will convert such a value to Clojure types.

  See https://docs.marklogic.com/javadoc/xcc/com/marklogic/xcc/types/package-summary.html"
  (reduce (fn [acc [k {:keys [xml-name ml->clj]}]]
            (assoc acc xml-name ml->clj))
          {}
          xcc-types))

(def variable-types
  "Mapping between Clojure keywords describing XML Schema types types
  and the Java representations of those types.

  See https://docs.marklogic.com/javadoc/xcc/com/marklogic/xcc/types/ValueType.html"
  (reduce (fn [acc [k {:keys [xcc-type]}]]
            (assoc acc k xcc-type))
          {}
          xcc-types))

(def xcc-type->xdm-conv-fn
  "Mapping from Clojure keywords describing XCC types to functions
  that will convert a Clojure value to an object appropriate for use
  in a MarkLogic XdmVariable."
  (reduce (fn [acc [k {:keys [clj->xdm]}]]
            (assoc acc k clj->xdm))
          {}
          xcc-types))

;;;;

(defn result->type
  "Returns type string of the given query Result object. Currently
  assumes result is homogenous."
  [result]
  (.toString (.getValueType (first (.toArray result)))))

(defn convert-types
  "Return the result of applying type conversion to the given
  MarkLogic query result sequence. Default type mappings can be
  overridden (in part or in whole) with the optional parameter
  `type-mapping`, which should contain a transformation function keyed
  by an XCC type string. See `xml-type-str->conv-fn` above."
  [result-sequence & [type-mapping]]
  ;; TODO throw informative exception if type not found in types
  (map (fn [item] (((merge xml-type-str->conv-fn
                          (when (map? type-mapping)
                            type-mapping))
                   (.toString (.getValueType item))) item))
       (.toArray result-sequence)))


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;;; Helpers for sessions and requests
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

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

(defn make-request-options
  "Creates a Request Options object (to pass to a Request or a
  Session) out of the given options map. See `valid-request-options`
  for supported keywords."
  [options]
  (let [request (RequestOptions.)]
    (when-not (every? valid-request-options (keys options))
      ;; TODO switch to spec
      (throw (IllegalArgumentException. "Invalid request option. Keywords passed in `options` must be a subset of `valid-request-options`.")))
    ;; TODO enforce types?
    (let [xs [[:auto-retry-delay-millis #(.setAutoRetryDelayMillis request %)]
              [:cache-result            #(.setCacheResult request %)]
              [:default-xquery-version  #(.setDefaultXQueryVersion request %)]
              [:effective-point-in-time #(.setEffectivePointInTime request (BigInteger. (str %)))]
              [:locale                  #(.setLocale request %)] ;; TODO enforce Locale object?
              [:max-auto-retry          #(.setMaxAutoRetry request (Integer. %))]
              [:query-language          #(.setQueryLanguage request %)]
              [:request-name            #(.setRequestName request %)]
              [:request-time-limit      #(.setRequestTimeLimit request %)]
              [:result-buffer-size      #(.setResultBufferSize request %)]
              [:timeout-millis          #(.setTimeoutMillis request %)]
              [:timezone                #(.setTimeZone request %)]]]
      (doseq [[k fn] xs]
        (when (contains? options k)
          (fn (k options)))))
    request))

(defn request-options->map
  "Given a RequestOptions object, returns a map describing those
  request options."
  [req-opts]
  {:auto-retry-delay-millis (.getAutoRetryDelayMillis req-opts)
   :cache-result (.getCacheResult req-opts)
   :default-xquery-version (.getDefaultXQueryVersion req-opts)
   :effective-point-in-time (.getEffectivePointInTime req-opts)
   :locale (.getLocale req-opts)
   :max-auto-retry (.getMaxAutoRetry req-opts)
   :query-language (.getQueryLanguage req-opts)
   :request-name (.getRequestName req-opts)
   :request-time-limit (.getRequestTimeLimit req-opts)
   :result-buffer-size (.getResultBufferSize req-opts)
   :timeout-millis (.getTimeoutMillis req-opts)
   :timezone (.getTimeZone req-opts)})


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;;; Session management
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn make-security-options
  "Given an SSLContext object and, optionally, a configuration map
  describing cipher suites and/or protocols to enable, returns a
  MarkLogic SecurityOptions object configured accordingly, to be used
  in session or content source creation."
  ([ssl-context] (make-security-options ssl-context nil))
  ([ssl-context {:keys [protocols cipher-suites]}]
   (let [sec-opts (SecurityOptions. ssl-context)]
     (when (seq protocols)
       (.setEnabledProtocols sec-opts (into-array String protocols)))
     (when (seq cipher-suites)
       (.setEnabledCipherSuites sec-opts (into-array String cipher-suites)))
     sec-opts)))

(defn security-options->map
  "Given a SecurityOptions object, returns a map describing its
  configuration."
  [security-options]
  {:hashcode (.hashCode security-options)
   :cipher-suites (.getEnabledCipherSuites security-options)
   :protocols (.getEnabledProtocols security-options)
   :ssl-context (.getSslContext security-options)})

(defn configure-content-source
  "Given a ContentSource object, modifies that object according to the
  given map of configuration options."
  [content-source {:keys [default-logger preemptive-auth]}]
  (when (instance? Logger default-logger)
    (.setDefaultLogger content-source default-logger))
  (when (or (true? preemptive-auth)
            (false? preemptive-auth))
    (.setAuthenticationPreemptive content-source preemptive-auth))
  content-source)

(defn make-uri-content-source
  "Return a ContentSource object according to the given `uri` and,
  optionally, a configuration map describing a SecurityOptions object,
  default Logger object, and boolean flag for whether basic
  authentication should be attempted preemptively. Accepts URI or
  string for `uri`."
  ([uri] (make-uri-content-source uri nil))
  ([uri {:keys [security-options default-logger preemptive-auth]}]
   (configure-content-source
    (if (instance? SecurityOptions security-options)
      (ContentSourceFactory/newContentSource (if (instance? URI uri)
                                               uri (URI. uri))
                                             security-options)
      (ContentSourceFactory/newContentSource (if (instance? URI uri)
                                               uri (URI. uri))))
    {:default-logger default-logger
     :preemptive-auth preemptive-auth})))

(defn make-hosted-content-source
  "Return a ContentSource object according to the given `host` String
  and integer `port`, and optionally a configuration map defining the
  `user` and `password`, `content-base`, `security-options`, and/or
  default Logger object and boolean flag for whether basic
  authentication should be attempted preemptively."
  ([host port] ;; no default login credentials
   (make-hosted-content-source host port nil))
  ([host port {:keys [user password content-base security-options
                      default-logger preemptive-auth]}]
   (configure-content-source (ContentSourceFactory/newContentSource
                              host port
                              ;; user and password must be together
                              (when (seq user)
                                user)
                              (when (seq user)
                                password)
                              (when (seq content-base)
                                content-base)
                              (when (instance? SecurityOptions security-options)
                                security-options))
                             {:default-logger default-logger
                              :preemptive-auth preemptive-auth})))

(defn make-cp-content-source
  "Given a ConnectionProvider, user, password, content-base, and an
  optional configuration map, returns a ContentSource object that will
  use the provided ConnectionProvider instance to obtain server
  connections.

  WARNING from the Javadoc: '[This function] should only be used by
  advanced users. A misbehaving ConnectionProvider implementation can
  result in connection failures and potentially even data loss. Be
  sure you know what you're doing.'"
  ([cxn-provider user password content-base]
   (make-cp-content-source cxn-provider user password content-base nil))
  ([cxn-provider user password content-base {:keys [default-logger
                                                    preemptive-auth]}]
   (when-not (instance? ConnectionProvider cxn-provider)
     (throw (IllegalArgumentException.
             "Content Source cxn-provider parameter must be a ConnectionProvider")))
   (configure-content-source (ContentSourceFactory/newContentSource cxn-provider
                                                                    user password
                                                                    content-base)
                             {:default-logger default-logger
                              :preemptive-auth preemptive-auth})))

(defn- create-session*
  "Creates session, given map of database info. If complex connection
  options are necessary, pass in a preconfigured content source."
  [{:keys [uri user password content-base]} & [content-source]]
  (let [cs (if (instance? ContentSource content-source)
             content-source
             (make-uri-content-source uri))]
    (cond
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
           (seq content-base)) (.newSession cs user password content-base))))

(def valid-session-config-options
  #{:default-request-options :logger :user-object
    :transaction-mode ;; XXX DEPRECATED, will be removed in future version
    :transaction-timeout :auto-commit? :update-mode})

(defn validate-session-config-options
  "Raises an error if the given configuration options are invalid for
  a MarkLogic session. See `valid-session-config-options` and
  https://docs.marklogic.com/javadoc/xcc/com/marklogic/xcc/Session.html"
  [config-options]
  (when-not (clojure.set/subset? (set (keys config-options))
                                 valid-session-config-options)
    (throw (IllegalArgumentException.
            (str "Unknown configuration parameter passed to `create-session`. Only "
                 valid-session-config-options
                 " are recognized keywords.")))))

(defn configure-session
  "Configures the given MarkLogic `session` according to the given
  `config-options`. See `create-session`."
  [session {:keys [default-request-options logger user-object
                   transaction-mode ;; XXX DEPRECATED, will be removed in future version
                   transaction-timeout auto-commit? update-mode]
            :as config-options}]
  (when (map? default-request-options)
    (.setDefaultRequestOptions session (make-request-options default-request-options)))
  (when (instance? Logger logger)
    (.setLogger session logger))
  ;; NB: the following is not the strictest test
  (when (instance? Object user-object)
    (.setUserObject session user-object))
  (when (keyword? transaction-mode) ;; XXX DEPRECATED
    (.setTransactionMode session (->transaction-mode transaction-mode)))
  (when (integer? transaction-timeout)
    (.setTransactionTimeout session transaction-timeout))
  (when (boolean? auto-commit?)
    (.setAutoCommit session auto-commit?))
  (when (or (keyword? update-mode) (boolean? update-mode))
    (.setUpdate session (->update-mode update-mode)))
  session)

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
  session options, though *default* request options can be set for the
  session.)

  If optional `content-source` is passed, the Session is created from
  the given ContentSource rather than creating one from the database
  info URI."
  ([db-info]
   (create-session* db-info))

  ([db-info {:keys [default-request-options logger user-object
                    transaction-mode ;; XXX DEPRECATED
                    transaction-timeout auto-commit? update-mode]
             :as config-options}]
   (validate-session-config-options config-options)
   (create-session db-info
                   nil
                   {:default-request-options default-request-options
                    :logger logger :user-object user-object
                    :transaction-mode transaction-mode ;; XXX DEPRECATED
                    :transaction-timeout transaction-timeout
                    :auto-commit? auto-commit?
                    :update-mode update-mode}))

  ([db-info content-source {:keys [default-request-options logger user-object
                                   transaction-mode ;; XXX DEPRECATED
                                   transaction-timeout auto-commit? update-mode]
                            :as config-options}]
   (validate-session-config-options config-options)
   (configure-session (create-session* db-info content-source) config-options)))

(defn create-default-session
  "Returns a session according to the default login credentials of the
  given `content-source`, which must be a
  com.marklogic.xcc.ContentSource object (see
  https://docs.marklogic.com/javadoc/xcc/com/marklogic/xcc/ContentSource.html),
  presumably created with one of Uruk's `make-*-content-source`
  functions. Optionally takes a map of `config-options` to configure
  the session."
  ([content-source]
   (create-default-session content-source nil))
  ([content-source {:keys [default-request-options logger user-object
                           transaction-mode ;; XXX DEPRECATED
                           transaction-timeout auto-commit? update-mode]
                    :as config-options}]
   (validate-session-config-options config-options)
   (configure-session (create-session* {} content-source) config-options)))

;; TODO require privilege
;; (defn session-cb-metadata
;;   "TODO"
;;   [session]
;;   (let [cbmd (.getContentbaseMetaData session)]
;;     {:session (.getSession cbmd) ;; XXX unnecessary...right?
;;      :user (.getUser cbmd)
;;      :cb-id (.getContentBaseId cbmd)
;;      :cb-name (.getContentBaseName cbmd)
;;      :driver-major-version (.getDriverMajorVersion cbmd)
;;      :driver-minor-version (.getDriverMinorVersion cbmd)
;;      :driver-patch-version (.getDriverPatchVersion cbmd)
;;      :driver-version (.getDriverVersionString cbmd)
;;      ;; :server-major-version (.getServerMajorVersion cbmd)
;;      ;; :server-minor-version (.getServerMinorVersion cbmd)
;;      ;; :server-patch-version (.getServerPatchVersion cbmd)
;;      ;; :server-version (.getServerVersionString cbmd)
;;      ;; :forest-ids (seq (.getForestIds cbmd))
;;      ;; :forest-maps (.getForestMap cbmd)
;;      ;; :forest-names (seq (.getForestNames cbmd))
;;      }
;;     ))

(defn user-credentials->map
  "Given a UserCredentials object, returns a map describing its
  configuration. See also methods `toHttpNegotiateAuth` and
  `toHttpDigestAuth`."
  [user-credentials]
  {:username (.getUserName user-credentials)
   :basic-auth (.toHttpBasicAuth user-credentials)
   :obj user-credentials})

(defn session->map
  "Returns a map describing configuration of the given Session
  object."
  [session]
  {:default-request-options (.getDefaultRequestOptions session)
   :effective-request-options (.getEffectiveRequestOptions session)
   :connection-uri (.getConnectionUri session)
   :logger (.getLogger session)
   ;; :contentbase-metadata (session-cb-metadata session) TODO
   :contentbase-name (.getContentBaseName session)
   :current-server-time (.getCurrentServerPointInTime session)
   :content-source (.getContentSource session)
   :xaresource (.getXAResource session)
   :user-object (.getUserObject session)
   :user-credentials (user-credentials->map (.getUserCredentials session))
   :closed? (.isClosed session) ;; TODO maybe create (defn closed? [session] ...) ? Except it wouldn't be specific to Session in this ns, and ResultSequence also has isClosed, so it's ambiguous.
   :cached-transaction-timeout (.getCachedTxnTimeout session)
   :transaction-timeout (.getTransactionTimeout session)
   :transaction-mode ((clojure.set/map-invert ->transaction-mode) (.getTransactionMode session)) ;; XXX DEPRECATED, will be removed in a later version
   :update-mode (try ((clojure.set/map-invert ->update-mode) (.getUpdate session))
                     (catch java.lang.NullPointerException npe
                       nil))
   :auto-commit? (.isAutoCommit session)})


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

(defn- make-request-obj
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
                                      (variable-types type)
                                      (if as-is?
                                        value
                                        ((xcc-type->xdm-conv-fn type) value)))
                     (.setNewVariable acc (name vname)
                                      (variable-types type)
                                      (if as-is?
                                        value
                                        ((xcc-type->xdm-conv-fn type) value))))))
               acc) ;; TODO use `doto` on `acc` in above block, instead of `setNewVariable` then returning `acc`?
             (doto request-factory (.setOptions (make-request-options options)))
             variables))

(defn submit-request
  "Construct, submit, and return raw results of request for the given
  `session` using `request-factory` and `query`. Modify it
  with (possibly empty) `options` and `variables` maps. Applies type
  conversion to response according to defaults and
  `xml-type-str->conv-fn`. Variables may be passed as a map of Strings
  or with String names corresponding to maps describing the variable
  using mandatory key `:value` and optional keys `:namespace` and
  `:type`.`"
  [request-factory session query options variables types shape]
  (let [req (sling/try+ (.submitRequest session
                                        (make-request-obj request-factory options variables))
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
  conversion in `xml-type-str->conv-fn`.

  Options passed must be in `valid-request-options` and conform to
  `request-options`.

  Variables may be passed as a map of Strings or with String names
  corresponding to maps describing the variable using mandatory key
  `:value` and optional keys `:namespace` and `:type`.`

  The shape of results is coerced using `shape-results` if the
  `:shape` key is passed. For example, a value of `:single` will
  return only the first value.

  Type conversion overrides must be a map using keys present in
  `uruk.core/xml-type-str->conv-fn` and conform to use in
  `convert-types`, that is, including values which are a function of
  one variable."
  ([session query]
   (execute-xquery session query {}))
  ([session query {:keys [options variables types shape]}]
   (submit-request (.newAdhocQuery session query) session query
                   options variables types shape)))

(defn execute-module
  "Execute the named module as a request to the database connection
  defined by the given session. Takes an optional configuration map
  describing request `options` and `variables`, desired `shape` of the
  result, and overrides of default type conversion in
  `xml-type-str->conv-fn`.

  Options passed must be in `valid-request-options` and conform to
  `request-options`.

  Variables may be passed as a map of Strings or with String names
  corresponding to maps describing the variable using mandatory key
  `:value` and optional keys `:namespace` and `:type`.`

  The shape of results is coerced using `shape-results` if the
  `:shape` key is passed. For example, a value of `:single` will
  return only the first value.

  Type conversion overrides must be a map using keys present in
  `uruk.core/xml-type-str->conv-fn` and conform to use in
  `convert-types`, that is, including values which are a function of
  one variable."
  ([session module]
   (execute-module session module {}))
  ([session module {:keys [options variables types shape]}]
   (submit-request (.newModuleInvoke session module) session module
                   options variables types shape)))

(defn spawn-module
  "Send the named module to the server to be run asynchronously, as a
  request to the database connection defined by the given session.

  Options passed must be in `valid-request-options` and conform to
  `request-options`.

  Variables may be passed as a map of Strings or with String names
  corresponding to maps describing the variable using mandatory key
  `:value` and optional keys `:namespace` and `:type`.`

  See https://docs.marklogic.com/javadoc/xcc/com/marklogic/xcc/ModuleSpawn.html"
  ([session module]
   (spawn-module session module {}))
  ;; FIXME test
  ;; NB, no response handling b/c asynchonous
  ([session module {:keys [options variables]}]
   (submit-request (.newModuleSpawn session module) session module
                   options variables)))

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

(defn make-content-permissions
  "Return an array of ContentPermissions decribing the given seq of
  content capability keys."
  [permissions]
  (into-array ContentPermission
              (reduce (fn [permissions-acc permission]
                        (conj permissions-acc
                              (ContentPermission.
                               (->content-capability (val (first permission)))
                               (key (first permission)))))
                      []
                      permissions)))

(defn content-creation-options
  "Creates a ContentCreateOptions object (to pass to a ContentFactory
  newContent call) out of the given options map. See
  `valid-content-creation-options` for supported keywords."
  [options]
  (let [cco (ContentCreateOptions.)]
    (when-not (every? valid-content-creation-options (keys options))
      ;; TODO switch to spec?
      (throw (IllegalArgumentException. "Invalid content creation option. Keywords passed in `options` must be a subset of `valid-content-creation-options`.")))
    (let [xs [[:buffer-size #(.setBufferSize cco %)]
              [:collections #(.setCollections cco (into-array String %))]
              [:encoding    #(.setEncoding cco %)]
              [:format      #(.setFormat cco (->doc-format %))]
              [:graph       #(.setGraph cco %)]
              [:language    #(.setLanguage cco %)]
              [:locale      #(.setLocale cco %)]
              [:namespace   #(.setNamespace cco %)]
              [:permissions #(.setPermissions cco (make-content-permissions %))]
              [:placement-keys      #(.setPlaceKeys cco %)]
              [:quality             #(.setQuality cco %)]
              [:repair-level        #(.setRepairLevel cco (->doc-repair-level %))]
              [:resolve-buffer-size #(.setResolveBufferSize cco %)]
              [:resolve-entities    #(.setResolveEntities cco %)]
              [:temporal-collection #(.setTemporalCollection cco %)]]]
      (doseq [[k fn] xs]
        (when-let [x (k options)]
          (fn x))))
    cco))

(defn content-creation-options->map
  [opts]
  {:buffer-size (.getBufferSize opts)
   :collections (mapv #(.toString %) (.getCollections opts))
   :encoding (.getEncoding opts)
   :format ((clojure.set/map-invert ->doc-format) (.getFormat opts))
   :graph (.getGraph opts)
   :language (.getLanguage opts)
   :locale (.getLocale opts)
   :namespace (.getNamespace opts)
   :permissions (mapv #(hash-map (.getRole %)
                                 (keyword (.toString (.getCapability %))))
                      (.getPermissions opts))
   :quality (.getQuality opts)
   :repair-level ((clojure.set/map-invert ->doc-repair-level) (.getRepairLevel opts))
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

(defn- is-json-string?
  "Returns true if given a valid JSON string; else false"
  [s]
  (if-not (string? s)
    false
    (let [s (.trim s)]
      (or (= s "true")
          (= s "false")
          (= s "null")
          (= (first s) \[)
          (= (first s) \{)
          (= (first s) \")
          (= (first s) \-) ;; if number is negative
          (number? (first s))))))

(defn- is-xml-string?
  "Returns true if `s` is a String containing valid XML; else false"
  [s]
  (if-not (string? s)
    false
    (try (if (doall (tree/flatten-elements [(xml/parse-str s)])) true false)
         (catch javax.xml.stream.XMLStreamException xmlse
           false)
         (catch Exception e
           false))))

(defn ->string-format
  "Returns a document format keyword describing given String."
  [s]
  (assert (string? s) "Parameter `s` must be a String")
  (cond (is-xml-string?  s) :xml
        (is-json-string? s) :json
        :else               :text))

(defn string->content
  "Given a String, returns a MarkLogic XCC Content object suitable for
  inserting to a database. Optionally takes a map of content creation
  options per `content-creation-options`. Determines content format
  using `->string-format`.

  See https://docs.marklogic.com/javadoc/xcc/com/marklogic/xcc/Content.html
  and https://docs.marklogic.com/javadoc/xcc/com/marklogic/xcc/ContentFactory.html"
  ([uri s]
   (string->content uri s {}))
  ([uri s options]
   (ContentFactory/newContent uri s
                              (content-creation-options (merge {:format (->string-format s)}
                                                               options)))))

(defn insert-string
  "Inserts the given String `s` at the given `uri` to the
  database/content-base according determined by the current
  `session`. Optionally takes a map of content creation options per
  `content-creation-options`.

  See https://docs.marklogic.com/javadoc/xcc/com/marklogic/xcc/Session.html#insertContent(com.marklogic.xcc.Content)"
  ([session uri s]
   (.insertContent session (string->content uri s)))
  ([session uri s options]
   (.insertContent session (string->content uri s options))))


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;;; Transactions
;;;;
;;;; Users must manage their own transactions, either from within
;;;; XQuery or programmatically. If working programmatically, wrapping
;;;; requests in `with-open` is strongly recommended. One may also use
;;;; the following functions within `with-open` after setting the
;;;; `auto-commit?` configuration option to `false` on the session.
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
