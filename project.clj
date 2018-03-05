(defproject uruk "0.3.11-SNAPSHOT"
  :description "Clojure wrapper of MarkLogic XML Content Connector For Java (XCC/J)"
  :url "https://github.com/daveliepmann/uruk"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.9.0"]
                 [com.marklogic/marklogic-xcc "9.0.4"]
                 ;; required but not included by MarkLogic (e.g. for ContentFactory):
                 [com.fasterxml.jackson.core/jackson-databind "2.9.4"]
                 [org.clojure/data.json "0.2.6"]
                 [org.clojure/data.xml "0.1.0-beta2"]
                 [slingshot "0.12.2"]]
  :checksum :warn ;; TODO remove this workaround -- MarkLogic's Maven repo doesn't have checksums?
  :repositories [["MarkLogic-releases" "http://developer.marklogic.com/maven2"]])
