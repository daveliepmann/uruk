(ns uruk.examples.readme
  "Examples from and for the README"
  (:require [uruk.core :as uruk]))

;; Basic usage
(with-open [session (uruk/create-session {:uri xdbc-uri :content-base database-name
                                          :user database-user :password database-pwd})]
  (uruk/execute-xquery session xquery-string))

;; Concrete example
(with-open [session (uruk/create-session {:uri "xdbc://localhost:8383/"
                                          :user "rest-writer" :password "password"})]
  (uruk/execute-xquery session "\"hello world\""))
;; => "hello world"

;; DB info
(def db {:uri "xdbc://localhost:8383/"
         :user "rest-admin" :password "password"
         :content-base "TutorialDB"})

(with-open [session (uruk/create-session db)]
  (uruk/execute-xquery session "\"hello world\"" :types :raw))
;; => #object[com.marklogic.xcc.impl.CachedResultSequence 0x2c034c22 "CachedResultSequence: size=1, closed=false, cursor=-1"]

;; Inspecting result types with `result-type`
(with-open [session (uruk/create-session db)]
  (uruk/result-type (uruk/execute-xquery session "\"hello world\"" :types :raw)))
;; => "xs:string"

;; Replacing the default type-conversion functions
(with-open [session (uruk/create-session db)]
  (uruk/execute-xquery session
                       "xquery version \"1.0-ml\"; doc('/dir/unwieldy.xml')"
                       :types {"document-node()" #(custom-function %)}))

;; Working within a transaction
(with-open [session (uruk/create-session db {:transaction-mode :update})]
  ;; The first request (query) starts a new, multi-statement transaction:
  (uruk/execute-xquery session "xdmp:document-insert('/docs/mst1.xml', <data><stuff/></data>)")
  
  ;; This second request executes in the same transaction as the
  ;; previous request and sees the results of the previous update:
  (uruk/execute-xquery session "xdmp:document-insert('/docs/mst2.xml', fn:doc(\"/docs/mst1.xml\"));")
  
  ;; After commit, updates are visible to other transactions. Commit
  ;; ends the transaction after current statement completes.
  (uruk/commit session) ;; <—- Transaction ends; updates are kept

  ;; Rollback discards changes and ends the transaction. The following
  ;; document deletion query never occurs, since it is rolled back
  ;; before calling commit:
  (uruk/execute-xquery session "xdmp:document-delete('/docs/mst1.xml')")
  (uruk/rollback session) ;; <– Transaction ends; updates are lost
  
  ;; Closing session without calling commit causes a rollback. The
  ;; following update is lost, since we don't commit before the end of
  ;; the (with-open) and its implicit `.close`:
  (uruk/execute-xquery session "xdmp:document-delete('/docs/mst1.xml')"))

;; Inserting Clojure XML elements
(with-open [session (uruk/create-session db)]
  (uruk/insert-element session "/content-factory/newcontent3" (clojure.data.xml/element :foo)))

(with-open [sess (uruk/create-session db)]
  (uruk/execute-xquery sess "xquery version \"1.0-ml\"; doc(\"/content-factory/newcontent3\")"))
