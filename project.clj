(defproject uruk "0.3.12-SNAPSHOT"
  :description "Clojure wrapper of MarkLogic XML Content Connector For Java (XCC/J)"
  :url "https://github.com/daveliepmann/uruk"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.9.0"]
                 [com.marklogic/marklogic-xcc "9.0.6"]
                 ;; required but not included by MarkLogic (e.g. for ContentFactory):
                 [com.fasterxml.jackson.core/jackson-databind "2.9.4"]
                 [org.clojure/data.json "0.2.6"]
                 [org.clojure/data.xml "0.1.0-beta2"]
                 [slingshot "0.12.2"]]
  :profiles {:codox {:dependencies [[codox-theme-rdash "0.1.2"]]
                     :plugins [[lein-codox "0.10.3"]]
                     :codox {:metadata {:doc/format :markdown} ;; docstring format
                             :output-path "gh-pages"
                             :themes [:rdash]}}}
  :repositories [["MarkLogic-releases" "http://developer.marklogic.com/maven2"]])
