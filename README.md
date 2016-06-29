# xray-charlie-charlie

A Clojure library for the MarkLogic XML Content Connector for Java (XCC/J). Currently in alpha. Not yet on Clojars.

The name comes from the [International Radiotelephony Spelling Alphabet](https://en.wikipedia.org/wiki/NATO_phonetic_alphabet)).

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
...which in this case should return:`["hello world"]`

## License

Copyright © 2016 David Liepmann

Distributed under the Eclipse Public License either version 1.0 or (at
your option) any later version.
