(defproject metabase/monetdb-driver "1.0.0-SNAPSHOT-1.0.0.jre8"
  :min-lein-version "2.5.0"

  :include-drivers-dependencies [#"^monetdb-jdbc-\d+\.jar$"]

  ; :dependencies
  ; [[monetdb/monetdb-jdbc "2.24"]
  ;  [clojure.java-time "0.3.2"]]

  :profiles
  {:provided
   {:dependencies
    [[org.clojure/clojure "1.10.1"]
     [metabase-core "1.0.0-SNAPSHOT"]
     [monetdb/monetdb-jdbc "2.24"]]}

   :uberjar
   {:auto-clean    true
    :aot           :all
    :javac-options ["-target" "1.8", "-source" "1.8"]
    :target-path   "target/%s"
    :uberjar-name  "monetdb.metabase-driver.jar"}})