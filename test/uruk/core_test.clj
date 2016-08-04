(ns uruk.core-test
  (:require [clojure.test :refer :all]
            [uruk.core :refer :all])
  (:import [java.util.logging Logger]
           [com.marklogic.xcc RequestOptions ContentSource]))

;; FIXME You'll have to fill in database credentials that work for
;; your system:
(def db {:uri "xdbc://localhost:8383/"
         :user "rest-admin" :password "x"
         :content-base "TutorialDB"})

(deftest create-session-parms
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

;;;; Request options

;; (deftest default-request-options
;;   (testing "A Request with no explicitly-set options must have default options"
;;     (let [req-opts (request-options {})]
;;       (describe-request-options req-opts))))

(deftest sample-request-options
  (testing "Set sample option on request"
    (is (= 6000
           (.getTimeoutMillis (request-options {:timeout-millis 6000}))))))

(deftest fail-on-invalid-request-option
  (testing "Options that don't exist must raise an error"
    (is (thrown? java.lang.IllegalArgumentException
                 (with-open [sess (create-session db)]
                   (execute-xquery sess "\"hello world\"" {:options {:reuqest-time-limt 500}}))))))

;; TODO a full end-to-end test of all request options

(defn is-default-session-options?
  "True if given session options conform to the expected default
  SessionOptions; false otherwise."
  [session-options]
  (and (is (instance? RequestOptions (:default-request-options session-options)))
       (is (instance? RequestOptions (:effective-request-options session-options)))
       (is (instance? Logger (:logger session-options))) ;; FIXME need better test
       (is (nil? (:user-object session-options)))
       (is (= 0 (:transaction-timeout session-options)))
       (is (nil? (:transaction-mode session-options)))))

;;;; Session options
(deftest default-session-options
  (testing "A session with no explicitly-set options must have default options"
    (testing "basic session creation with full database specified"
      (is-default-session-options? (session-options (create-session db))))
    (testing "session creation with URI content source"
      (is-default-session-options? (session-options
                                    (create-session
                                     db (make-uri-content-source "xdbc://localhost:8383/")
                                     {}))))
    (testing "session creation with URI content source using options"
      (is-default-session-options? (session-options
                                    (create-session
                                     db (make-uri-content-source "xdbc://localhost:8383/"
                                                                 {:preemptive-auth true})
                                     {}))))
    (testing "session creation with hosted content source"
      (is-default-session-options? (session-options
                                    (create-session
                                     db (make-hosted-content-source "localhost" 8383)
                                     {}))))
    (testing "session creation with hosted content source using options"
      (is-default-session-options? (session-options
                                    (create-session
                                     db (make-hosted-content-source "localhost" 8383
                                                                    {:content-base "TutorialDB"})
                                     {}))))
    ;; TODO once we want to delve into extreme complexity of ConnectionProvider
    ;; (testing "session creation with connectionProvider content source"
    ;;   (is-default-session-options? (session-options
    ;;                                 (create-session
    ;;                                  db (make-cp-content-source ...)
    ;;                                  {}))))
    ;; TODO once we want to delve into extreme complexity of ConnectionProvider
    ;; (testing "session creation with connectionProvider content source with options"
    ;;   (is-default-session-options? (session-options
    ;;                                 (create-session
    ;;                                  db (make-cp-content-source ...)
    ;;                                  {}))))
    ))

(defn as-expected-session-options?
  [session-options expected-options]
  (and (is (= (:timeout-millis (:default-request-options expected-options))
              (.getTimeoutMillis (:default-request-options session-options))
              (.getTimeoutMillis (:effective-request-options session-options))))
       (is (instance? Logger (:logger session-options))) ;; TODO test Logger better--name?
       (is (empty? (:user-object session-options))) ;; XXX is this all we can test user-object?
       (is (= (:transaction-timeout session-options)
              (:transaction-timeout expected-options)))
       (is (= (:transaction-mode session-options)
              (:transaction-mode expected-options)))))

(deftest set-session-options
  (testing "A session with explicitly-set options must reflect those options"
    (let [opts {:default-request-options {:timeout-millis 75}
                ;; TODO test default Req Opts more?
                ;; TODO test effective Req Opts more?
                :transaction-timeout 56
                :transaction-mode :query}]
      (testing "with standard database map"
        (as-expected-session-options? (session-options
                                       (create-session db opts))
                                      opts))
      (testing "with uri content source"
        (as-expected-session-options? (session-options
                                       (create-session db
                                                       (make-uri-content-source "xdbc://localhost:8383/")
                                                       opts))
                                      opts))
      (testing "with uri content source using options"
        (as-expected-session-options? (session-options
                                       (create-session
                                        db (make-uri-content-source "xdbc://localhost:8383/"
                                                                    {:preemptive-auth false})
                                        opts))
                                      opts))
      (testing "with hosted content source"
        (as-expected-session-options? (session-options
                                       (create-session
                                        db (make-hosted-content-source "localhost" 8383)
                                        opts))
                                      opts))
      (testing "with hosted content source using content base"
        (as-expected-session-options? (session-options
                                       (create-session
                                        db (make-hosted-content-source "localhost" 8383
                                                                       {:content-base "TutorialDB"})
                                        opts))
                                      opts))
      (testing "with hosted content source using user, password, content-base"
        (as-expected-session-options? (session-options
                                       (create-session
                                        db (make-hosted-content-source "localhost" 8383
                                                                       {:user "rest-admin"
                                                                        :password "x"
                                                                        :content-base "TutorialDB"})
                                        opts))
                                      opts))
      (testing "with hosted content source using user and password"
        (as-expected-session-options? (session-options
                                       (create-session
                                        db (make-hosted-content-source "localhost" 8383
                                                                       {:user "rest-admin"
                                                                        :password "x"})
                                        opts))
                                      opts))
      ;; TODO once we want to delve into extreme complexity of ConnectionProvider
      ;; (as-expected-session-options? (session-options
      ;;                                (create-session
      ;;                                 db (make-cp-content-source ...)
      ;;                                 opts))
      ;;                               opts)
      )))

;; TODO accept-only-valid-session-options


;;;; Content creation options

;; TODO content creation options default

(deftest content-options-roundtrip
  (testing "Round-trip options through creation and description"
    (is (let [opts {:buffer-size 400
                    :collections ["my-collection" "another-collection"]
                    :encoding "ASCII"
                    :format :text
                    :graph "my-graph"
                    :language "fr"
                    ;; :locale
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
             (describe-content-creation-options (content-creation-options opts)))))))

;; TODO accept-only-valid-content-options

;;;; Variables

(deftest non-string-variables
  (testing "Non-string variables passed to request object are not converted to strings"
    (is (instance? com.marklogic.xcc.types.impl.DocumentImpl
                   (-> (with-open [session (create-session db)]
                         (.getVariables (#'uruk.core/request-obj (.newAdhocQuery session "hello world")
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

;;;; TODO Type conversion

;;;; Invalid query

(deftest error-on-invalid-query
  (testing "An error must be thrown if MarkLogic is passed an invalid query."
    (is (thrown? java.lang.Exception
                 ;; FIXME I'd love to get the original error type
                 ;; here, e.g. XqueryException
                 (with-open [sess (create-session db)]
                   (execute-xquery sess "let $uri := xdmp:get-request-field(\"uri\")returnif"))))))

;;;; TODO Shape

;;;; TODO Transactions

;;;; TODO element insertion


;;;; TODO security options
;; (requires SSLContext)



;;;; Content Source from URI

;; (comment
;;   (session-options (create-session {:uri "xdbc://localhost:8383/"
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
;;   (session-options (create-session {:uri "xdbc://localhost:8383/"
;;                                     :user "rest-admin" :password "x"
;;                                     :content-base "TutorialDB"}
;;                                    (hosted-content-source "localhost" 8383
;;                                                           "rest-admin" "x" "TutorialDB"
;;                                                           (security-options TODO))
;;                                    {}))

;;   (session-options (create-session {:uri "xdbc://localhost:8383/"
;;                                     :user "rest-admin" :password "x"
;;                                     :content-base "TutorialDB"}
;;                                    (managed-content-source connection-provider ;; TODO implement https://docs.marklogic.com/javadoc/xcc/com/marklogic/xcc/spi/ConnectionProvider.html interface
;;                                                            "rest-admin" "x" "TutorialDB")
;;                                    {})))
