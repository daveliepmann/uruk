(ns uruk.examples.readme
  "Examples from and for the README"
  (:require [uruk.core :as uruk])
  (:import [com.marklogic.xcc ValueFactory]))

(comment

  ;; Basic usage
  (with-open [session (uruk/create-session {:uri xdbc-uri :content-base database-name
                                            :user database-user :password database-pwd})]
    (uruk/execute-xquery session xquery-string))

  ;; Concrete example
  (with-open [session (uruk/create-session {:uri "xdbc://localhost:8383/"
                                            :user "uruk-tester" :password "password"})]
    (uruk/execute-xquery session "\"hello world\""))
  ;; => ("hello world")

  ;; DB info
  (def db {:uri "xdbc://localhost:8383/"
           :user "uruk-tester" :password "password"
           :content-base "UrukDB"})

  ;; Lots of functionality is in the optional config map:
  (with-open [session (uruk/create-session db)]
    (uruk/execute-xquery session
                         "xquery version \"1.0-ml\"; doc('/bigdoc.xml')"
                         {:types :raw
                          :options {:cache-result false}
                          :variables {:a "a"}
                          :shape :single}))


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Types
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

  (with-open [session (uruk/create-session db)]
    (uruk/execute-xquery session "\"hello world\"" {:types :raw}))
  ;; => #object[com.marklogic.xcc.impl.CachedResultSequence 0x2c034c22 "CachedResultSequence: size=1, closed=false, cursor=-1"]

  ;; Inspecting result types with `result->type`
  (with-open [session (uruk/create-session db)]
    (uruk/result->type (uruk/execute-xquery session "\"hello world\"" {:types :raw})))
  ;; => "xs:string"

  ;; Replacing the default type-conversion functions
  (with-open [session (uruk/create-session db)]
    (uruk/execute-xquery session
                         "xquery version \"1.0-ml\"; doc('/dir/unwieldy.xml')"
                         {:types {"document-node()" #(custom-function %)}}))


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Shape
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

  (with-open [session (uruk/create-session {:uri "xdbc://localhost:8383/"
                                            :user "uruk-tester" :password "password"})]
    (uruk/execute-xquery session "\"hello world\"" {:shape :single}))



;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Inserting elements
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

  ;; XXX This section is out of order compared to the README, since we
  ;; use the inserted document momentarily in the Options section.

  ;; Insert a document with a Content Creation Option:
  (with-open [session (uruk/create-session db)]
    (uruk/insert-element session
                         "/content-factory/new-doc"
                         (clojure.data.xml/element :foo)
                         {:quality 2}))


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Options
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

  (with-open [sess (uruk/create-session db)]
    (uruk/execute-xquery sess "xquery version \"1.0-ml\"; doc('/content-factory/new-doc')"
                         {:types :raw
                          :options {:cache-result false}}))
  ;; => #object[com.marklogic.xcc.impl.StreamingResultSequence 0x6d7f6 "StreamingResultSequence: closed=true"]


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Variables
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

  ;; Simple XS_STRING variables:
  (with-open [session (uruk/create-session db)]
    (uruk/execute-xquery session "xquery version \"1.0-ml\";
                                declare variable $my-variable as xs:string external;
                                $my-variable"
                         {:variables {"my-variable" "my-value"}
                          :shape :single!}))

  ;; Or use maps describing the variable:
  (with-open [session (uruk/create-session db)]
    (uruk/execute-xquery session "xquery version \"1.0-ml\";
                                declare variable $my-variable as xs:integer external;
                                $my-variable"
                         {:variables {"my-variable" {:value 1
                                                     :type :xs-integer}}
                          :shape :single!}))

  ;; Automatic wrapping/conversion of many variable types:
  (with-open [session (uruk/create-session db)]
    (uruk/execute-xquery session "xquery version \"1.0-ml\";
                                declare variable $my-variable as boolean-node() external;
                                $my-variable"
                         {:variables {"my-variable" {:value false
                                                     :type :boolean-node}}
                          :shape :single!}))

  ;; As-is:
  (with-open [session (uruk/create-session db)]
    (uruk/execute-xquery session "xquery version \"1.0-ml\";
                           declare variable $my-variable as boolean-node() external;
                           $my-variable"
                         {:variables {"my-variable" {:value (-> (com.fasterxml.jackson.databind.node.JsonNodeFactory/instance)
                                                                (.booleanNode false)
                                                                ValueFactory/newBooleanNode)
                                                     :type :boolean-node
                                                     :as-is? true}}
                          :shape :single!}))


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Working within a transaction
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

  ;; Open a session and configure it to trigger multi-statement transaction use:
  (with-open [session (uruk/create-session db {:auto-commit? false :update-mode true})]
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

;;;; Inserting Clojure XML elements
  (with-open [session (uruk/create-session db)]
    (uruk/insert-element session "/content-factory/new-doc" (clojure.data.xml/element :foo)))

  (with-open [sess (uruk/create-session db)]
    (uruk/execute-xquery sess "xquery version \"1.0-ml\"; doc(\"/content-factory/new-doc\")"))

  )
