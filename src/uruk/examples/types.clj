(ns uruk.examples.types
  "Type examples, mostly for demonstrating conversions."
  (:require [uruk.core :refer :all]))

(comment

  (def session
    (create-session {:uri "xdbc://localhost:8383/"
                     :user "uruk-tester" :password "password"
                     :content-base "UrukDB"}))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;;; Interfaces for JSON
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

  ;; ArrayNode
  (execute-xquery session "xquery version \"1.0-ml\"; array-node {1,2,3}")

  ;; BooleanNode
  (execute-xquery session "xquery version \"1.0-ml\"; boolean-node{false()}")

  ;; TODO how to produce JsonItem?

  ;; NullNode
  (execute-xquery session "xquery version \"1.0-ml\"; null-node {}")

  ;; NumberNode
  (execute-xquery session "xquery version \"1.0-ml\"; number-node {1}")

  ;; ObjectNode
  (execute-xquery session "let $object := json:object()
                         let $_ := map:put($object,\"a\",111)
                         return xdmp:to-json($object)")


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;;; JS
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

  ;; JSON Array
  (execute-xquery session "xquery version \"1.0-ml\";
          json:array(
           <json:array xmlns:json=\"http://marklogic.com/xdmp/json\"
           xmlns:xs=\"http://www.w3.org/2001/XMLSchema\"
           xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\">
           <json:value xsi:type=\"xs:string\">hello</json:value>
           <json:value xsi:type=\"xs:string\">world</json:value>
           <json:array>
           <json:value xsi:type=\"xs:string\">one</json:value>
           <json:value xsi:type=\"xs:string\">two</json:value>
           </json:array>
           </json:array>
          )")

  ;; JSON Object
  (execute-xquery session "xquery version \"1.0-ml\";
          json:object()")


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;;; CTS types
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

  (execute-xquery session "cts:box(45, -122, 78, 30)")

  (execute-xquery session "cts:circle(20, cts:point(37.655983, -122.425525))")

  (execute-xquery session "cts:point(37.655983, -122.425525)")

  (execute-xquery session "(: this polygon approximates the 94041 zip code :)
  let $points := (cts:point(0.373899653086420E+02, -0.122078578406509E+03),
    cts:point(0.373765400000000E+02, -0.122063772000000E+03),
    cts:point(0.373781400000000E+02, -0.122067972000000E+03),
    cts:point(0.373825650000000E+02, -0.122068365000000E+03),
    cts:point(0.373797400000000E+02, -0.122072172000000E+03),
    cts:point(0.373899400000000E+02, -0.122092573000000E+03),
    cts:point(0.373941400000000E+02, -0.122095573000000E+03),
    cts:point(0.373966400000000E+02, -0.122094173000000E+03),
    cts:point(0.373958400000000E+02, -0.122092373000000E+03),
    cts:point(0.374004400000000E+02, -0.122091273000000E+03),
    cts:point(0.374004400000000E+02, -0.122091273000000E+03),
    cts:point(0.373873400000000E+02, -0.122057872000000E+03),
    cts:point(0.373873400000000E+02, -0.122057872000000E+03),
    cts:point(0.373854400000000E+02, -0.122052672000000E+03),
    cts:point(0.373833400000000E+02, -0.122053372000000E+03),
    cts:point(0.373819400000000E+02, -0.122057572000000E+03),
    cts:point(0.373775400000000E+02, -0.122060872000000E+03),
    cts:point(0.373765400000000E+02, -0.122063772000000E+03) )
  return
  cts:polygon($points)")


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;;; XDM types
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

  ;; TODO

  ;; XDMBinary
  (comment (execute-xquery session "xquery version \"1.0-ml\"; xdmp:document-load(\"/path/to/mlfavicon.png\");")
           (execute-xquery session "xquery version \"1.0-ml\"; doc(\"/path/to/mlfavicon.png\");"))



;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;;; XS types
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

  ;; XSAnyURI
  (execute-xquery session "fn:resolve-uri(\"hello/goodbye.xml\",
                                                 \"http://mycompany/default.xqy\")")

  ;; XSBase64Binary
  (execute-xquery session "xs:base64Binary(\"bmhnY2p2\")")

  ;; XSBoolean
  (execute-xquery session "fn:doc-available(\"derp\")") ;

  ;; XSDate
  (execute-xquery session "fn:current-date()")

  ;; XSDateTime
  (execute-xquery session "fn:current-dateTime()")

  ;; XSDayTimeDuration
  ;; this fn removed in version 1.0; only used to get correct response type
  (execute-xquery session "xquery version \"0.9-ml\"
                          fn:subtract-dateTimes-yielding-dayTimeDuration(fn:current-dateTime(), xs:dateTime(\"2000-01-11T12:01:00.000Z\"))")

  ;; XSDecimal
  (execute-xquery session "fn:abs(-1.2)")

  ;; XSDouble
  (execute-xquery session "fn:number(-1.2)")

  (execute-xquery session "xs:double(-1.2)")

  ;; XSDuration
  (execute-xquery session "xs:duration(\"P3DT10H\")")

  ;; XSFloat
  (execute-xquery session "xs:float(\"1\")")

;;;; Gregorians
  ;; XSGDay
  (execute-xquery session "xs:gDay('---08')")

  ;; XSGMonth
  (execute-xquery session "xs:gMonth('--08')")

  ;; XSGMonthDay
  (execute-xquery session "xs:gMonthDay('--08-20')")

  ;; XSGYear
  (execute-xquery session "xs:gYear('2016')")

  ;; XSGYearMonth
  (execute-xquery session "xs:gYearMonth('2016-02')")
;;;;; end Gregorians

  ;; HexBinary
  (execute-xquery session "xs:hexBinary(\"74657374\")")

  (execute-xquery session "xdmp:integer-to-hex(string-to-codepoints(\"Testing binary Constructor\"))")

  (execute-xquery session "data(xdmp:subbinary(binary { xs:hexBinary(\"DEADBEEF\") }, 3, 2))")

  ;; XSInteger (plural)
  (execute-xquery session "xdmp:databases()")

  ;; XSQName
  (execute-xquery session "fn:QName(\"http://www.example.com/example\", \"person\")")

  ;; String
  (execute-xquery session "\"hello world\"")

  ;; Time
  (execute-xquery session "fn:current-time()" {:shape :single!})

  ;; Untyped Atomic
  (execute-xquery session "let $x as xs:untypedAtomic*
                           := (xs:untypedAtomic(\"cherry\"),
                               xs:untypedAtomic(\"1\"),
                               xs:untypedAtomic(\"1\"))
                         return fn:distinct-values ($x)")

  ;; YearMonthDuration
  (execute-xquery session "xquery version \"0.9-ml\"
                         fn:subtract-dateTimes-yielding-yearMonthDuration(fn:current-dateTime(), xs:dateTime(\"2000-01-11T12:01:00.000Z\"))"
                  {:shape :single})


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
  (.close session)

  )
