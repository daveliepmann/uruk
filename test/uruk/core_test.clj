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

(deftest session-parms-1
  (testing "Create session with just URI"
    (is (= "Hello world"
           (let [session (create-session db)]
             (-> session
                 (.submitRequest (.newAdhocQuery session
                                                 "\"Hello world\""))
                 .asString))))))

(deftest session-parms-2
  (testing "Create session with URI and content-base"
    (is (= "Hello world"
           (let [session (create-session db)]
             (-> session
                 (.submitRequest (.newAdhocQuery session
                                                 "\"Hello world\""))
                 .asString))))))

(deftest session-parms-3
  (testing "Create session with URI, username, password"
    (is (= "Hello world"
           (let [session (create-session db)]
             (-> session
                 (.submitRequest (.newAdhocQuery session
                                                 "\"Hello world\""))
                 .asString))))))

(deftest session-parms-4
  (testing "Create session with all non-options parameters"
    (is (= "Hello world" 
           (let [session (create-session db)]
             (-> session
                 (.submitRequest (.newAdhocQuery session
                                                 "\"Hello world\""))
                 .asString))))))

;;;; Request options

;; (deftest default-request-options
;;   (testing "A Request with no explicitly-set options must have default options"
;;     (let [req-opts (request-options {})]
;;       (describe-request-options req-opts))))

(deftest sample-request-options
  (testing "Set sample option on request"
    (is (= 6000
           (.getTimeoutMillis (request-options {:timeout-millis 6000}))))))

(deftest accept-only-valid-request-options
  (testing "Options that don't exist must raise an error"
    (is (thrown? java.lang.IllegalArgumentException
                 (with-open [sess (create-session db)]
                   (execute-xquery sess "\"hello world\"" {:options {:reuqest-time-limt 500}}))))))



;; TODO a full end-to-end test of all request options

;;;; Session options
(deftest default-session-options
  (testing "A session with no explicitly-set options must have default options"
    (let [sess-opts (session-options (create-session db))]
      (and (is (instance? RequestOptions (:default-request-options sess-opts)))
           (is (instance? RequestOptions (:effective-request-options sess-opts)))
           (is (instance? Logger (:logger sess-opts)))
           (is (nil? (:user-object sess-opts)))
           (is (= 0 (:transaction-timeout sess-opts)))
           (is (nil? (:transaction-mode sess-opts)))))))

(deftest set-session-options
  (testing "A session with explicitly-set options must reflect those options"
    (let [sess-opts (session-options
                     (create-session db
                                     {:default-request-options {:timeout-millis 75}
                                      ;; TODO test user-object
                                      ;; TODO test Logger more deeply
                                      ;; TODO test default Req Opts more?
                                      ;; TODO test effective Req Opts more?
                                      :transaction-timeout 56
                                      :transaction-mode :query}))]
      (and (is (= 75
                  (.getTimeoutMillis (:default-request-options sess-opts))
                  (.getTimeoutMillis (:effective-request-options sess-opts))))
           (is (instance? Logger (:logger sess-opts)))
           (is (empty? (:user-object sess-opts)))
           (is (= 56 (:transaction-timeout sess-opts)))
           (is (= :query (:transaction-mode sess-opts)))))))

;; TODO accept-only-valid-session-options

;; TODO all these session creation approaches
;; (session-options (create-session {:uri "xdbc://localhost:8383/"
;;                                   :user "rest-admin" :password "x"
;;                                   :content-base "TutorialDB"}
;;                                  {} (uri-content-source "xdbc://localhost:8383/")))

;; (comment
;;   (session-options (create-session {:uri "xdbc://localhost:8383/"
;;                                     :user "rest-admin" :password "x"
;;                                     :content-base "TutorialDB"}
;;                                    {} (uri-content-source "xdbc://localhost:8383/"
;;                                                           (security-options TODO)))))

;; (session-options (create-session {:uri "xdbc://localhost:8383/"
;;                                   :user "rest-admin" :password "x"
;;                                   :content-base "TutorialDB"}
;;                                  {} (hosted-content-source "localhost" 8383
;;                                                            "rest-admin" "x" "TutorialDB")))

;; (comment
;;   (session-options (create-session {:uri "xdbc://localhost:8383/"
;;                                     :user "rest-admin" :password "x"
;;                                     :content-base "TutorialDB"}
;;                                    {} (hosted-content-source "localhost" 8383
;;                                                              "rest-admin" "x" "TutorialDB"
;;                                                              (security-options TODO))))

;;   (session-options (create-session {:uri "xdbc://localhost:8383/"
;;                                     :user "rest-admin" :password "x"
;;                                     :content-base "TutorialDB"}
;;                                    {} (managed-content-source connection-provider ;; TODO implement https://docs.marklogic.com/javadoc/xcc/com/marklogic/xcc/spi/ConnectionProvider.html interface
;;                                                               "rest-admin" "x" "TutorialDB"))))


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
