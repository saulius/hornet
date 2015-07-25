(def hbase-version "1.0.0-cdh5.4.4")

(defproject hornet "0.1.1"
  :description "A modern Clojure HBase client"
  :url "http://github.com/saulius/hornet"
  :license {:name "MIT License"
            :url "http://opensource.org/licenses/MIT"}
  :dependencies [[org.apache.hbase/hbase-client ~hbase-version]
                 [com.taoensso/encore "1.38.0"]]
  :profiles {:dev {:dependencies [[org.clojure/clojure "1.7.0"]
                                  [criterium "0.4.3"]]}}
  :repositories [["cloudera" "https://repository.cloudera.com/artifactory/cloudera-repos/"]]
  :global-vars {*warn-on-reflection* true})
