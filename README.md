# xray-charlie-charlie

A Clojure library for the MarkLogic XML Content Connector for Java (XCC/J). Currently in alpha. Not yet on Clojars.

The name comes from the [International Radiotelephony Spelling Alphabet](https://en.wikipedia.org/wiki/NATO_phonetic_alphabet).

Sponsored by [LambdaWerk](https://lambdawerk.com/home).

## Usage

Basic usage takes the form of:
``` clojure
(with-open [session (create-session {:uri xdbc-uri :content-base database-name
                                     :user database-user :password database-pwd})]
  (execute-xquery session xquery-string))
```
...of which a concrete example is:
``` clojure
(with-open [session (create-session {:uri "xdbc://localhost:8383/"
                                     :user "rest-writer" :password "password"})]
  (execute-xquery session "\"hello world\""))
```
...which in this case should return `"hello world"` (if you provide valid credentials).

Let's `def` our database information for concision's sake:
``` clojure
(def db {:uri "xdbc://localhost:8383/"
         :user "rest-admin" :password "password"
         :content-base "TutorialDB"})
```

### Types
Basic type conversion is performed automatically for most [XCC types](https://docs.marklogic.com/javadoc/xcc/com/marklogic/xcc/types/package-summary.html). If for some reason you need access to the raw results, pass `:raw` to the optional typed parameter `:types`, like so, using the database info we just defined:
``` clojure
(with-open [session (create-session db)]
  (execute-xquery session "\"hello world\"" :types :raw))

=> #object[com.marklogic.xcc.impl.CachedResultSequence 0x2c034c22 "CachedResultSequence: size=1, closed=false, cursor=-1"]
```

This lets you inspect result types with `result-type`:
``` clojure
(with-open [session (create-session db)]
  (result-type (execute-xquery session "\"hello world\"" :types :raw)))

=> "xs:string"
```

Those result types form the keys to the `types` map, whose values are functions used to transform result items into more manageable Clojure types. For most types that’s as simple as `"document-node()" #(.asString %)` (for XdmDocuments) or reading the number contained in a string. But if you need more in-depth handling of results, you can override the default mappings–a la carte!–by passing a map to the aforementioned `types` parameter, like so:

``` clojure
(with-open [session (create-session db)]
  (execute-xquery session
                  "xquery version \"1.0-ml\"; doc('/dir/unwieldy.xml')"
                  :types {"document-node()" #(custom-function %)}))
```

### Transactions

Multiple database updates that must occur together can take advantage of transactions. To borrow an example from the XCC Developer’s Guide:

>The following example demonstrates using multi-statement transactions in Java. The first multi-statement transaction in the session inserts two documents into the database, calling Session.commit to complete the transaction and commit the updates. The second transaction demonstrates the use of Session.rollback. The third transaction demonstrates implicitly rolling back updates by closing the session.

– [Programming in XCC > Multi-Statement Transactions](https://docs.marklogic.com/guide/xcc/concepts#id_35788)

We translate the original Java to Clojure, taking advantage of Clojure’s `with-open` idiom:

``` clojure
(with-open [session (create-session db {:transaction-mode :update})]
  ;; The first request (query) starts a new, multi-statement transaction:
  (execute-xquery session "xdmp:document-insert('/docs/mst1.xml', <data><stuff/></data>)")
  
  ;; This second request executes in the same transaction as the
  ;; previous request and sees the results of the previous update:
  (execute-xquery session "xdmp:document-insert('/docs/mst2.xml', fn:doc(\"/docs/mst1.xml\"));)")
  
  ;; After commit, updates are visible to other transactions. Commit
  ;; ends the transaction after current statement completes.
  (commit session) ;; <—- Transaction ends; updates are kept

  ;; Rollback discards changes and ends the transaction. The following
  ;; document deletion query never occurs, since it is rolled back
  ;; before calling commit:
  (execute-xquery session "xdmp:document-delete('/docs/mst1.xml')")
  (rollback session) ;; <– Transaction ends; updates are lost
  
  ;; Closing session without calling commit causes a rollback. The
  ;; following update is lost, since we don't commit before the end of
  ;; the (with-open) and its implicit `.close`:
  (execute-xquery session "xdmp:document-delete('/docs/mst1.xml')"))
```

## License

Copyright © 2016 David Liepmann

Distributed under the Eclipse Public License either version 1.0 or (at
your option) any later version.
