(ns uruk.core-test
  (:require [clojure.test :refer :all]
            [uruk.core :refer :all])
  (:import [java.util.logging Logger]
           [java.util Locale TimeZone]
           [com.marklogic.xcc RequestOptions]))

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

(deftest default-request-options
  (testing "A Request with no explicitly-set options must have default options"
    (is (= (let [req-opts (request-options {})]
             (describe-request-options req-opts))
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

(deftest sample-request-options
  (testing "Set sample option on request"
    (is (= 6000
           (.getTimeoutMillis (request-options {:timeout-millis 6000}))))))

(deftest roundtrip-request-options
  (testing "All request options must be set as indicated"
    (is (= (let [req-opts (request-options {:timezone (TimeZone/getTimeZone "Pacific/Chuuk")
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
             (describe-request-options req-opts))

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

(deftest accept-only-valid-request-options
  (testing "Options that don't exist must raise an error"
    (is (thrown? java.lang.IllegalArgumentException
                 (with-open [sess (create-session db)]
                   (execute-xquery sess "\"hello world\"" {:options {:reuqest-time-limt 500}}))))))


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


;; (deftest accept-only-valid-session-options
;;   (testing "Invalid session options should throw an error"
;;     (let [sess-opts (session-options
;;                      (create-session db
;;                                      {:this-does-not-exist "irrelevant string"}))])))

;; TODO accept-only-valid-session-options


;;;; Content creation options

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
           (describe-content-creation-options (content-creation-options {}))))))

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
             (describe-content-creation-options (content-creation-options opts)))))))

(deftest accept-only-valid-content-options
  (testing "Content Options that don't exist must raise an error"
    (is (thrown? java.lang.IllegalArgumentException
                 (content-creation-options {:does-not-exist "irrelevant"})))))


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

;;;; Element insertion

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

;; TODO test element insertion with content creation option, e.g. quality=2
