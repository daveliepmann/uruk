(ns uruk.core-test
  (:require [clojure.test :refer :all]
            [uruk.core :refer :all])
  (:import [java.util.logging Logger]
           [java.util Locale TimeZone]
           [com.marklogic.xcc RequestOptions ContentSource]))

;; FIXME You'll have to fill in database credentials that work for
;; your system:
(def db {:uri "xdbc://localhost:8383/"
         :user "rest-admin" :password "x"
         :content-base "TutorialDB"})

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;;; Request options
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(deftest default-request-options
  (testing "Creating a request with no explicitly-set options must reflect default options"
    (is (= (request-options->map (make-request-options {}))
           {:timezone nil,
            :cache-result true,
            :locale nil,
            :request-time-limit -1,
            :default-xquery-version nil,
            :timeout-millis -1,
            :query-language nil,
            :result-buffer-size 0,
            :effective-point-in-time nil,
            :request-name nil,
            :auto-retry-delay-millis -1,
            :max-auto-retry -1}))))

(deftest create-session-with-parms
  (testing "Creating sessions with a variety of parameters"
    (testing "...with just URI"
      (is (thrown? IllegalStateException
                   (let [session (create-session {:uri "xdbc://localhost:8383/"})]
                     (.submitRequest session (.newAdhocQuery session "\"Hello world\""))))))

    (testing "...with URI and content-base"
      (is (thrown? IllegalStateException
                   (let [session (create-session {:uri "xdbc://localhost:8383/"
                                                  :content-base "TutorialDB"})]
                     (.submitRequest session (.newAdhocQuery session "\"Hello world\""))))))

    (testing "...with URI, user, password"
      (is (= "Hello world"
             (let [session (create-session {:uri "xdbc://localhost:8383/"
                                            :user "rest-admin" :password "x"})]
               (-> session
                   (.submitRequest (.newAdhocQuery session
                                                   "\"Hello world\""))
                   .asString)))))

    (testing "...with URI, user, password, content-base"
      (is (= "Hello world"
             (let [session (create-session db)]
               (-> session
                   (.submitRequest (.newAdhocQuery session
                                                   "\"Hello world\""))
                   .asString)))))))

(deftest sample-request-options
  (testing "Set sample option on request"
    (is (= 6000
           (.getTimeoutMillis (make-request-options {:timeout-millis 6000}))))))

(deftest roundtrip-request-options
  (testing "All request options must be set as indicated"
    (is (= (let [req-opts (make-request-options {:timezone (TimeZone/getTimeZone "Pacific/Chuuk")
                                                 :cache-result false,
                                                 :locale (Locale. "ru"),
                                                 :request-time-limit 156,
                                                 :default-xquery-version "xquery version \"0.9-ml\";",
                                                 :timeout-millis 763,
                                                 :query-language "Elvish"
                                                 :result-buffer-size 23,
                                                 :effective-point-in-time 14701453805890320
                                                 :request-name "JigoroKano",
                                                 :auto-retry-delay-millis 991,
                                                 :max-auto-retry 3})]
             (request-options->map req-opts))
           {:timezone (TimeZone/getTimeZone "Pacific/Chuuk")
            :cache-result false,
            :locale (Locale. "ru"),
            :request-time-limit 156,
            :default-xquery-version "xquery version \"0.9-ml\";",
            :timeout-millis 763,
            :query-language "Elvish"
            :result-buffer-size 23,
            :effective-point-in-time 14701453805890320,
            :request-name "JigoroKano",
            :auto-retry-delay-millis 991,
            :max-auto-retry 3}))))

(deftest fail-on-invalid-request-option
  (testing "Options that don't exist must raise an error"
    (is (thrown? java.lang.IllegalArgumentException
                 (with-open [sess (create-session db)]
                   (execute-xquery sess "\"hello world\"" {:options {:reuqest-time-limt 500}}))))))


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;;; Session configuration
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn is-default-session-config?
  "True if given session configuration conforms to the expected
  default Session; false otherwise."
  [session]
  (and (is (instance? RequestOptions (:default-request-options session)))
       (is (instance? RequestOptions (:effective-request-options session)))
       (is (instance? Logger (:logger session))) ;; FIXME need better test
       (is (nil? (:user-object session)))
       (is (= 0 (:transaction-timeout session)))
       (is (nil? (:transaction-mode session)))))

(deftest default-session-config
  (testing "A session with no explicitly-set configuration must have default configuration"
    (testing "basic session creation with full database specified"
      (is-default-session-config? (session->map (create-session db))))
    (testing "session creation with URI content source"
      (is-default-session-config? (session->map
                                   (create-session
                                    db (make-uri-content-source "xdbc://localhost:8383/")
                                    {}))))
    (testing "session creation with URI content source using options"
      (is-default-session-config? (session->map
                                   (create-session
                                    db (make-uri-content-source
                                        "xdbc://localhost:8383/"
                                        {:preemptive-auth false
                                         :default-logger (Logger/getLogger "foobar")})))))
    (testing "session creation with hosted content source"
      (is-default-session-config? (session->map
                                   (create-session
                                    db (make-hosted-content-source "localhost" 8383)
                                    {}))))
    (testing "session creation with hosted content source using options"
      (is-default-session-config? (session->map
                                   (create-session
                                    db (make-hosted-content-source "localhost" 8383
                                                                   {:content-base "TutorialDB"})
                                    {}))))
    ;; TODO once we want to delve into extreme complexity of ConnectionProvider
    ;; (testing "session creation with connectionProvider content source"
    ;;   (is-default-session-config? (session->map
    ;;                                 (create-session
    ;;                                  db (make-cp-content-source ...)
    ;;                                  {}))))
    ;; TODO once we want to delve into extreme complexity of ConnectionProvider
    ;; (testing "session creation with connectionProvider content source with options"
    ;;   (is-default-session-config? (session->map
    ;;                                 (create-session
    ;;                                  db (make-cp-content-source ...)
    ;;                                  {}))))
    ))

;; (deftest accept-only-valid-session-options
;;   (testing "Invalid session config options should throw an error"
;;     (let [sess-opts (session-options
;;                      (create-session db
;;                                      {:this-does-not-exist "irrelevant string"}))])))


(defn as-expected-session-config?
  "True if given `session` is configured as according to
  `expected-config`; false otherwise."
  [session expected-config]
  (and (is (= (:timeout-millis (:default-request-options expected-config))
              (.getTimeoutMillis (:default-request-options session))
              (.getTimeoutMillis (:effective-request-options session))))
       (is (instance? Logger (:logger session))) ;; TODO test Logger better--name?
       (is (empty? (:user-object session))) ;; XXX is this all we can test user-object?
       (is (= (:transaction-timeout session)
              (:transaction-timeout expected-config)))
       (is (= (:transaction-mode session)
              (:transaction-mode expected-config)))))

(deftest set-session-config
  (testing "A session with explicitly-set configuration must reflect that configuration"
    (let [opts {:default-request-options {:timeout-millis 75}
                ;; TODO test default Req Opts more?
                ;; TODO test effective Req Opts more?
                :transaction-timeout 56
                :transaction-mode :query}]
      (testing "with standard database map"
        (as-expected-session-config? (session->map
                                      (create-session db opts))
                                     opts))
      (testing "with uri content source"
        (as-expected-session-config? (session->map
                                      (create-session db
                                                      (make-uri-content-source "xdbc://localhost:8383/")
                                                      opts))
                                     opts))
      (testing "with uri content source using options"
        (as-expected-session-config? (session->map
                                      (create-session
                                       db (make-uri-content-source "xdbc://localhost:8383/"
                                                                   {:preemptive-auth false})
                                       opts))
                                     opts))
      (testing "with hosted content source"
        (as-expected-session-config? (session->map
                                      (create-session
                                       db (make-hosted-content-source "localhost" 8383)
                                       opts))
                                     opts))
      (testing "with hosted content source using content base"
        (as-expected-session-config? (session->map
                                      (create-session
                                       db (make-hosted-content-source "localhost" 8383
                                                                      {:content-base "TutorialDB"})
                                       opts))
                                     opts))
      (testing "with hosted content source using user, password, content-base"
        (as-expected-session-config? (session->map
                                      (create-session
                                       db (make-hosted-content-source "localhost" 8383
                                                                      {:user "rest-admin"
                                                                       :password "x"
                                                                       :content-base "TutorialDB"})
                                       opts))
                                     opts))
      (testing "with hosted content source using user and password"
        (as-expected-session-config? (session->map
                                      (create-session
                                       db (make-hosted-content-source "localhost" 8383
                                                                      {:user "rest-admin"
                                                                       :password "x"})
                                       opts))
                                     opts))
      ;; TODO once we want to delve into extreme complexity of ConnectionProvider
      ;; (as-expected-session-config? (session->map
      ;;                                (create-session
      ;;                                 db (make-cp-content-source ...)
      ;;                                 opts))
      ;;                               opts)
      )))

;; TODO accept-only-valid-session-config


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;;; Content creation options
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(deftest default-content-options
  (testing "Passing nothing to content creation options should return default options"
    (is (= {:encoding "UTF-8"
            :format :none,
            :permissions []
            :buffer-size -1
            :locale nil
            :repair-level :default
            :resolve-buffer-size 0
            :collections []
            :language nil
            :resolve-entities false
            :graph nil
            :quality 0
            :namespace nil
            :temporal-collection nil}
           (content-creation-options->map (content-creation-options {}))))))

(deftest content-options-roundtrip
  (testing "Round-trip options through creation and description"
    (is (let [opts {:buffer-size 400
                    :collections ["my-collection" "another-collection"]
                    :encoding "ASCII"
                    :format :text
                    :graph "my-graph"
                    :language "fr"
                    :locale (Locale. "ru")
                    :namespace "my-ns"
                    :permissions [{"such-and-such-role" :insert}
                                  {"such-and-such-role" :update}]
                    ;; :placement-keys
                    :quality 20
                    :repair-level :full
                    :resolve-buffer-size 20
                    :resolve-entities true
                    :temporal-collection "my-temp"}]
          (= opts
             (content-creation-options->map (content-creation-options opts)))))))

(deftest accept-only-valid-content-options
  (testing "Content Options that don't exist must raise an error"
    (is (thrown? java.lang.IllegalArgumentException
                 (content-creation-options {:does-not-exist "irrelevant"})))))


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;;; Variables
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(deftest non-string-variables
  (testing "Non-string variables passed to request object are not converted to strings"
    (is (instance? com.marklogic.xcc.types.impl.DocumentImpl
                   (-> (with-open [session (create-session db)]
                         (.getVariables (#'uruk.core/make-request-obj
                                         (.newAdhocQuery session "hello world")
                                         nil {:derp {:value "<foo/>"
                                                     :type :document}})))
                       first
                       .getValue)))))

(deftest as-is-boolean-variable
  (testing "Clojure booleans automatically convert to correct XdmVariable type"
    (is (false? (with-open [session (create-session db)]
                  (execute-xquery session "xquery version \"1.0-ml\";
                                declare variable $my-variable as boolean-node() external;
                                $my-variable"
                                  {:variables {"my-variable" {:value false
                                                              :type :boolean-node}}
                                   :shape :single!}))))))

;; TODO more variable testing
;; TODO test `as-is?`
;; TODO test (all?) variable types
;; TODO test default xs_string map structure


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;;; TODO Type conversion
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;;; Invalid query
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(deftest error-on-invalid-query
  (testing "An error must be thrown if MarkLogic is passed an invalid query."
    (is (thrown? java.lang.Exception
                 ;; FIXME I'd love to get the original error type
                 ;; here, e.g. XqueryException
                 (with-open [sess (create-session db)]
                   (execute-xquery sess "let $uri := xdmp:get-request-field(\"uri\")returnif"))))))


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;;; TODO Shape
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;;; TODO Transactions
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;;; Element insertion
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(deftest can-insert-doc
  (testing "Clojure XML Elements must be insertable as documents"
    ;; Insert a document
    (with-open [session (create-session db)]
      (insert-element session
                      "/content-factory/new-doc"
                      (clojure.data.xml/element :foo)))
    ;; Did that insert work?
    (is (= "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<foo/>"
           (with-open [session (create-session db)]
             (execute-xquery session
                             "xquery version \"1.0-ml\"; fn:doc('/content-factory/new-doc');"
                             {:shape :single}))))
    ;; Clean up inserted document so we don't affect the next test run
    (with-open [session (create-session db)]
      (execute-xquery session
                      "xquery version \"1.0-ml\"; xdmp:document-delete('/content-factory/new-doc');"
                      {:shape :single}))))

(deftest can-insert-doc-with-options
  (testing "Insert-element must accept content creation options"
    ;; Insert a document with various options
    (with-open [session (create-session db)]
      (insert-element session
                      "/content-factory/new-doc"
                      (clojure.data.xml/element :foo)
                      {:quality 2})) ;; TODO test other options too?
    ;; Did our options get included in that doc?
    (is (= 2
           (with-open [session (create-session db)]
             (execute-xquery session
                             "xquery version \"1.0-ml\";
                              xdmp:document-get-quality('/content-factory/new-doc');"
                             {:shape :single}))))
    ;; Clean up inserted document so we don't affect the next test run
    (with-open [session (create-session db)]
      (execute-xquery session
                      "xquery version \"1.0-ml\";
                       xdmp:document-delete('/content-factory/new-doc');"
                      {:shape :single}))))


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;;; TODO security options
;; (requires SSLContext)
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;;; Content Source
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

;; (comment
;;   (session->map (create-session {:uri "xdbc://localhost:8383/"
;;                                     :user "rest-admin" :password "x"
;;                                     :content-base "TutorialDB"}
;;                                    {} (uri-content-source "xdbc://localhost:8383/"
;;                                                           (security-options TODO)))))

(deftest content-source-creation-with-uri
  (testing "content source creation from just a URI"
    (let [cs (make-uri-content-source "xdbc://localhost:8383/")]
      (and (instance? ContentSource cs)
           (= 8383 (.getPort (.getConnectionProvider cs)))
           (= "localhost" (.getHostName (.getConnectionProvider cs)))))))

;; TODO content source from URI and securityoptions
;; (deftest content-source-creation-with-uri-and-security-options
;;   (testing "content source creation from just a URI"
;;     (let [cs (uri-content-source "xdbc://localhost:8383/"
;;                                  (make-security-options FIXME))]
;;       (and (instance? ContentSource cs)
;;            (= 8383 (.getPort (.getConnectionProvider (uri-content-source "xdbc://localhost:8383/"))))
;;            (= "localhost" (.getHostName (.getConnectionProvider (uri-content-source "xdbc://localhost:8383/"))))))))

(deftest content-source-creation-with-host-and-port
  (testing "content source creation from host and port"
    (let [cs (make-hosted-content-source "localhost" 8383)]
      (and (instance? ContentSource cs)
           (= 8383 (.getPort (.getConnectionProvider cs)))
           (= "localhost" (.getHostName (.getConnectionProvider cs)))))))

(deftest content-source-creation-with-host-port-user-pwd
  (testing "content source creation from host and port"
    (let [cs (make-hosted-content-source "localhost" 8383
                                         {:user "rest-admin" :password "x"})]
      (and (instance? ContentSource cs)
           (= 8383 (.getPort (.getConnectionProvider cs)))
           (= "localhost" (.getHostName (.getConnectionProvider cs)))))))

;; TODO with SSLContext
;; (comment
;;   (describe-session-options (create-session {:uri "xdbc://localhost:8383/"
;;                                     :user "rest-admin" :password "x"
;;                                     :content-base "TutorialDB"}
;;                                    (hosted-content-source "localhost" 8383
;;                                                           "rest-admin" "x" "TutorialDB"
;;                                                           (security-options TODO))
;;                                    {}))

;;   (describe-session-options (create-session {:uri "xdbc://localhost:8383/"
;;                                     :user "rest-admin" :password "x"
;;                                     :content-base "TutorialDB"}
;;                                    (managed-content-source connection-provider ;; TODO implement https://docs.marklogic.com/javadoc/xcc/com/marklogic/xcc/spi/ConnectionProvider.html interface
;;                                                            "rest-admin" "x" "TutorialDB")
;;                                    {})))

(deftest content-src-options
  (testing "Content Source preemptive-authentication settings"
    (is (true? (.isAuthenticationPreemptive (make-uri-content-source "xdbc://localhost:8383/"
                                                                     {:preemptive-auth true}))))
    (is (false? (.isAuthenticationPreemptive (make-uri-content-source "xdbc://localhost:8383/"
                                                                      {:preemptive-auth false}))))
    (is (true? (.isAuthenticationPreemptive (make-hosted-content-source "localhost" 8383
                                                                        {:preemptive-auth true}))))
    (is (false? (.isAuthenticationPreemptive (make-hosted-content-source "localhost" 8383
                                                                         {:preemptive-auth false}))))
    ;; TODO with make-cp-content-source, once we want to delve into
    ;; the extreme complexity of ConnectionProvider
    )
  (testing "Content Source default Logger settings"
    (is (= "foo" (.getName (.getDefaultLogger (make-uri-content-source
                                               "xdbc://localhost:8383/"
                                               {:default-logger (Logger/getLogger "foo")})))))
    (is (= "bar" (.getName (.getDefaultLogger (make-hosted-content-source
                                               "localhost" 8383
                                               {:default-logger (Logger/getLogger "bar")})))))
    ;; TODO with make-cp-content-source, once we want to delve into
    ;; the extreme complexity of ConnectionProvider
    ))
