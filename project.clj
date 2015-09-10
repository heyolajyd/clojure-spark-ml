(defproject clojurespark-ml "0.1.0-SNAPSHOT"
  :description "FIXME: write description"
  :url "http://example.com/FIXME"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.6.0"]
                 [clj-http "1.1.2"]
                 [yieldbot/flambo "0.6.0"]
                 [org.apache.spark/spark-core_2.10 "1.4.0"]
                 [org.apache.spark/spark-mllib_2.10 "1.4.0"]]
  :aot :all
  :main ^:skip-aot clojurespark-ml.core
  :target-path "target/%s"
  :jvm-opts ^:replace ["-server" "-Xmx2g"]
  :profiles {:uberjar {:aot :all}})
