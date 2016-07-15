(ns uruk.core-test
  (:require [clojure.test :refer :all]
            [uruk.core :refer :all]))

;; FIXME You'll have to fill in database credentials that work for
;; your system:
(def db {:uri "xdbc://localhost:8383/"
         :user "rest-writer" :password "password"
         :content-base "TutorialDB"})

(deftest session-parms-1
  (testing "Cannot create session with just URI"
    (is (= "Hello world"
           (let [session (create-session db)]
             (-> session
                 (.submitRequest (.newAdhocQuery session
                                                 "\"Hello world\""))
                 .asString))))))

(deftest session-parms-2
  (testing "Cannot create session with URI and content-base"
    (is (= "Hello world"
           (let [session (create-session db)]
             (-> session
                 (.submitRequest (.newAdhocQuery session
                                                 "\"Hello world\""))
                 .asString))))))

(deftest session-parms-3
  (testing "Cannot create session with URI, username, password"
    (is (= "Hello world"
           (let [session (create-session db)]
             (-> session
                 (.submitRequest (.newAdhocQuery session
                                                 "\"Hello world\""))
                 .asString))))))

(deftest session-parms-4
  (testing "Cannot create session with all non-options parameters"
    (is (= "Hello world" 
           (let [session (create-session db)]
             (-> session
                 (.submitRequest (.newAdhocQuery session
                                                 "\"Hello world\""))
                 .asString))))))

