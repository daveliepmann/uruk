# xray-charlie-charlie

A Clojure library for the MarkLogic XML Content Connector for Java (XCC/J). Currently in alpha. Not yet on Clojars.

The name comes from the [International Radiotelephony Spelling Alphabet](https://en.wikipedia.org/wiki/NATO_phonetic_alphabet).

Sponsored by [LambdaWerk](https://lambdawerk.com/home).

## Usage

Basic usage takes the form of:
``` clojure
(with-open [session (create-session xdbc-uri db-usr db-pwd db-name)]
  (execute-xquery session xquery-string))
```
...of which a concrete example is:
``` clojure
(with-open [session (create-session "xdbc://localhost:8383/"
                                    "rest-admin" "x" "TutorialDB")]
  (execute-xquery session "\"hello world\""))
```
...which in this case should return:`"hello world"`

### Transactions

Multiple database updates that must occur together can take advantage of transactions. To borrow an example from the XCC Developer’s Guide:

>The following example demonstrates using multi-statement transactions in Java. The first multi-statement transaction in the session inserts two documents into the database, calling Session.commit to complete the transaction and commit the updates. The second transaction demonstrates the use of Session.rollback. The third transaction demonstrates implicitly rolling back updates by closing the session."

– [Programming in XCC > Multi-Statement Transactions](https://docs.marklogic.com/guide/xcc/concepts#id_35788)

We translate the original Java to Clojure, taking advantage of Clojure’s `with-open` idiom:

``` clojure
(with-open [session (create-session "xdbc://blahblah:8383/"
                                    "user" "password" "databaseName"
                                    {:transaction-mode :update})]
  ;; The first request (query) starts a new, multi-statement transaction:
  (execute-xquery session "xdmp:document-insert('/docs/mst1.xml', <data><stuff/></data>)")
  
  ;; This second request executes in the same transaction as the
  ;; previous request and sees the results of the previous update:
  (execute-xquery session "xdmp:document-insert('/docs/mst2.xml', fn:doc(\"/docs/mst1.xml\"));)")
  
  ;; After commit, updates are visible to other transactions. Commit
  ;; ends the transaction after current statement completes.
  (.commit session) ;; <—- Transaction ends; updates are kept

  ;; Rollback discards changes and ends the transaction. The following
  ;; document deletion query never occurs, since it is rolled back
  ;; before calling commit:
  (execute-xquery session "xdmp:document-delete('/docs/mst1.xml')")
  (.rollback session) ;; <– Transaction ends; updates are lost
  
  ;; Closing session without calling commit causes a rollback. The
  ;; following update is lost, since we don't commit before the end of
  ;; the (with-open) and its implicit `.close`:
  (execute-xquery session "xdmp:document-delete('/docs/mst1.xml')"))
```

## License

Copyright © 2016 David Liepmann

Distributed under the Eclipse Public License either version 1.0 or (at
your option) any later version.
