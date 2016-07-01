(ns xray-charlie-charlie.core
  "Marklogic XCC core functions: session management, querying..."
  (:import [java.util.logging Logger]
           [com.marklogic.xcc ContentSourceFactory
            Session$TransactionMode
            RequestOptions]
           [com.marklogic.xcc.types ValueType]
           java.net.URI))

;;;; Helpers 
(def transaction-modes
  "Mapping of keywords for valid Session transaction modes (per
  https://docs.marklogic.com/javadoc/xcc/com/marklogic/xcc/Session.TransactionMode.html)."
  {:auto Session$TransactionMode/AUTO
   :query Session$TransactionMode/QUERY
   :update Session$TransactionMode/UPDATE
   :update-auto-commit Session$TransactionMode/UPDATE_AUTO_COMMIT})

(def valid-request-options
  "Set of valid request options for Sessions or Request objects.
  See https://docs.marklogic.com/javadoc/xcc/com/marklogic/xcc/RequestOptions.html"
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
    (if (every? valid-request-options (keys options))
      (do (when-let [ardm (:auto-retry-delay-millis options)]
            (.setAutoRetryDelayMillis request ardm))
          (when (or (true? (:cache-result options))
                    (false? (:cache-result options)))
            (.setCacheResult request (:cache-result options)))
          (when-let [dxv (:default-xquery-version options)]
            (.setDefaultXQueryVersion request dxv))
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
          request)
      
      ;; TODO switch to spec
      (throw (IllegalArgumentException. "Invalid request option. Keywords passed in `options` must be a subset of `valid-request-options`.")))))

(defn- configure-session
  "Configures the given Session object according to given options
  map. Assumes correct types are passed: map
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

(defn- create-session*
  "Create a session (with no options) according to methods provided by
  ContentSource:
  https://docs.marklogic.com/javadoc/xcc/com/marklogic/xcc/ContentSource.html"
  ([uri] 
   (.newSession (ContentSourceFactory/newContentSource (URI. uri))))
  ([uri content-base]
   (.newSession (ContentSourceFactory/newContentSource (URI. uri)) content-base))
  ([uri user pwd]
   (.newSession (ContentSourceFactory/newContentSource (URI. uri)) user pwd)) 
  ([uri user pwd content-base] ;; <-- this is probably the one you want.
   (.newSession (ContentSourceFactory/newContentSource (URI. uri)) user pwd content-base)))


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;;; Session management
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

;; FIXME there is currently no way to pass URI, usr, pwd, plus content-base, with no options
(defn create-session
  "Create a Session according to the given parameters, configured
  according to the given options map.
  
  See https://docs.marklogic.com/javadoc/xcc/com/marklogic/xcc/Session.html
  for valid options. (Note that request options are distinct from
  session options, though default request options *can* be set for the session.)
  
  See https://docs.marklogic.com/javadoc/xcc/com/marklogic/xcc/ContentSource.html
  for allowed parameter arrangements."
  ([uri options]
   (-> (create-session* uri)
       (configure-session options)))
  ([uri content-base options]
   (-> (create-session* uri content-base)
       (configure-session options)))
  ([uri user pwd options]
   (-> (create-session* uri user pwd)
       (configure-session options))) 
  ([uri user pwd content-base options]
   (-> (create-session* uri user pwd content-base)
       (configure-session options))))


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;;; XQuery Requests
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn submit-request
  "Construct, submit, and return raw results of request for the given
  `session` using `request-factory` and `query`. Modify it
  with (possibly empty) `options` and `variables` maps."
  [request-factory session query options variables]
  (let [ro      (request-options options)
        request (reduce-kv (fn [acc vname vval]
                             (.setNewVariable acc (name vname)
                                              ValueType/XS_STRING (str vval))
                             acc)
                           (doto request-factory (.setOptions ro))
                           options)]
    (.submitRequest session request)))

(defn execute-xquery
  "Execute the given xquery query as a request to the database
  connection defined by the given session. Apply request options or
  variables if given."
  ([session query]
   (submit-request (.newAdhocQuery session query) session query {} {}))
  ([session query options]
   (submit-request (.newAdhocQuery session query) session query options {}))
  ([session query options variables]
   (submit-request (.newAdhocQuery session query) session query options variables)))

(defn execute-module
  "Execute the named module as a request to the database connection
  defined by the given session. Apply request options or variables if
  given."
  ([session module]
   (submit-request (.newModuleInvoke session module) session module {} {}))
  ([session module options]
   (submit-request (.newModuleInvoke session module) session module options {}))
  ([session module options variables]
   (submit-request (.newModuleInvoke session module) session module options variables)))


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;;; Transactions
;;;;
;;;; Users must manage their own transactions, either from within
;;;; XQuery or programmatically. If working programmatically, one may
;;;; use these functions after setting the transaction mode to
;;;; `:update` or `:query` on the session via the `:transaction-mode`
;;;; option. Note that `execute-query` uses `.submitQuery` under the
;;;; hood.
;;;; 
;;;; See https://docs.marklogic.com/guide/xcc/concepts#id_23310
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn open-transaction
  "Returns a newly started transaction, optionally setting `name` or `time-limit`
  (in seconds)."
  ([client] (.openTransaction client))
  ([client name] (.openTransaction client name))
  ([client name time-limit] (.openTransaction client name time-limit)))

(defn commit
  "Commit `transaction` when it successfully finishes."
  [transaction]
  (.commit transaction))

(defn rollback
  "Rollback a multi-statement transaction to reset any actions that
  have already occured in that transaction; for example, delete any
  created items, restore any deleted items, revert back any edits,
  etc."
  [transaction]
  (.rollback transaction))


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;;; Miscellania
;;;; Convenience functions and the like
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn default-permissions
  "Convenience function to return the permissions any new document
  would get if the current user were to insert a document without
  specifying the default permissions.

  See https://docs.marklogic.com/xdmp:default-permissions"
  ;; TODO convert from XML to JSON
  ;; TODO maybe look up names of returned role-ids?
  [uri user pwd content-base options]
  (with-open [session (create-session uri user pwd content-base options)]
    (let [query "xdmp:default-permissions()"]
      (apply str (map #(.asString %)
                      (.toArray (submit-request (.newAdhocQuery session query)
                                                session query {} {})))))))
