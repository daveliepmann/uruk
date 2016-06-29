(ns xray-charlie-charlie.core-test
  (:require [clojure.test :refer :all]
            [xray-charlie-charlie.core :refer :all]))

(deftest session-parms-1
  (testing "Cannot create session with just URI"
    (is (= "Hello world"
           (let [session (create-session "xdbc://rest-admin:x@localhost:8383/" {})]
             (-> session
                 (.submitRequest (.newAdhocQuery session
                                                 "\"Hello world\""))
                 .asString))))))

(deftest session-parms-2
  (testing "Cannot create session with URI and content-base"
    (is (= "Hello world"
           (let [session (create-session "xdbc://rest-admin:x@localhost:8383/"
                                         "Documents" {})]
             (-> session
                 (.submitRequest (.newAdhocQuery session
                                                 "\"Hello world\""))
                 .asString))))))

(deftest session-parms-3
  (testing "Cannot create session with URI, username, password"
    (is (= "Hello world"
           (let [session (create-session "xdbc://localhost:8383/"
                                         "rest-admin" "x" {})]
             (-> session
                 (.submitRequest (.newAdhocQuery session
                                                 "\"Hello world\""))
                 .asString))))))

(deftest session-parms-4
  (testing "Cannot create session with all non-options parameters"
    (is (= "Hello world" 
           (let [session (create-session "xdbc://localhost:8383/"
                                         "rest-admin" "x" "Documents" {})]
             (-> session
                 (.submitRequest (.newAdhocQuery session
                                                 "\"Hello world\""))
                 .asString))))))

