(defproject xray-charlie-charlie "0.1.0-SNAPSHOT"
  :description "FIXME: write description"
  :url "http://example.com/FIXME"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.8.0"]
                 [com.marklogic/marklogic-xcc "8.0.5"]
                 ;; marklogic does not require the following, so we
                 ;; have to pull it ourselves, even if we do not USE
                 ;; IT. being used in a file that types a static
                 ;; method parameter with this is enough.
                 [com.fasterxml.jackson.core/jackson-databind "2.6.3"]]
  :repositories [["MarkLogic-releases" "http://developer.marklogic.com/maven2"]])
