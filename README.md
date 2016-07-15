# uruk

A Clojure library wrapping the MarkLogic XML Content Connector for Java (XCC/J). Made to help you access your Enterprise NoSQL database from Clojure.

Named after the [ancient Mesopotamian city-state](http://www.metmuseum.org/toah/hd/uruk/hd_uruk.htm) and [period](http://www.metmuseum.org/toah/hd/wrtg/hd_wrtg.htm) in which some of the oldest known writing has been found. One can see Uruk as perhaps the first document database—and it certainly wasn’t organized relationally.

Sponsored by [LambdaWerk](https://lambdawerk.com/home).

## Installation

[![Clojars Project](https://img.shields.io/clojars/v/uruk.svg)](https://clojars.org/uruk)

In your *project.clj* dependencies: `[uruk "0.1.0"]`

In your namespace: `(:require [uruk.core :as uruk])`. (I also like `ur` as an alias, for brevity. Delightfully, Ur is another [ancient city-state with ties to the origins of written documents](https://en.wikipedia.org/wiki/Ur).)

## Usage

For ease of replication, the examples below are also in `/src/uruk/examples/readme.clj`.

Basic usage takes the form of:
``` clojure
(with-open [session (uruk/create-session {:uri xdbc-uri :content-base database-name
                                          :user database-user :password database-pwd})]
  (uruk/execute-xquery session xquery-string))
```
...of which a concrete example is:
``` clojure
(with-open [session (uruk/create-session {:uri "xdbc://localhost:8383/"
                                          :user "rest-writer" :password "password"})]
  (uruk/execute-xquery session "\"hello world\""))
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
(with-open [session (uruk/create-session db)]
  (uruk/execute-xquery session "\"hello world\"" :types :raw))

=> #object[com.marklogic.xcc.impl.CachedResultSequence 0x2c034c22 "CachedResultSequence: size=1, closed=false, cursor=-1"]
```

This lets you inspect result types with `result-type`:
``` clojure
(with-open [session (uruk/create-session db)]
  (uruk/result-type (uruk/execute-xquery session "\"hello world\"" :types :raw)))

=> "xs:string"
```

Those result types form the keys to the `types` map, whose values are functions used to transform result items into more manageable Clojure types. For most types that’s as simple as `"document-node()" #(.asString %)` (for XdmDocuments) or reading the number contained in a string. But if you need more in-depth handling of results, you can override the default mappings–a la carte!–by passing a map to the aforementioned `types` parameter, like so:

``` clojure
(with-open [session (uruk/create-session db)]
  (uruk/execute-xquery session
                       "xquery version \"1.0-ml\"; doc('/dir/unwieldy.xml')"
                       :types {"document-node()" #(custom-function %)}))
```

### Transactions

Multiple database updates that must occur together can take advantage of transactions. To borrow an example from the XCC Developer’s Guide:

>The following example demonstrates using multi-statement transactions in Java. The first multi-statement transaction in the session inserts two documents into the database, calling Session.commit to complete the transaction and commit the updates. The second transaction demonstrates the use of Session.rollback. The third transaction demonstrates implicitly rolling back updates by closing the session.

– [Programming in XCC > Multi-Statement Transactions](https://docs.marklogic.com/guide/xcc/concepts#id_35788)

We translate the original Java to Clojure, taking advantage of Clojure’s `with-open` idiom:

``` clojure
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
```

### Inserting Clojure XML Elements

You can insert `clojure.data.xml.Element`s as content:

``` clojure
(with-open [session (uruk/create-session db)]
  (uruk/insert-element session
                       "/content-factory/newcontent3" ;; uri to insert at
                       (clojure.data.xml/element :foo)))
```
This function takes an optional map describing document metadata to use during the insert.

## TODO
  - revise tests for new db connection scheme
  - more tests
  - spec?
  - ensure insert-element robustly covers needed use cases
  - with-session macro?

## License

Copyright © 2016 David Liepmann

Distributed under the Eclipse Public License either version 1.0 or (at
your option) any later version.
