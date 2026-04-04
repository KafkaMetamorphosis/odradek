(defproject odradek "0.1.0-SNAPSHOT"
  :description "Odradek - Kafka SLO Metrics Exporter"
  :dependencies [[org.clojure/clojure "1.12.0"]
                 [com.stuartsierra/component "1.2.0"]
                 [ring/ring-core "1.12.2"]
                 [ring/ring-jetty-adapter "1.12.2"]
                 [ring/ring-json "0.5.1"]
                 [compojure "1.7.1"]
                 [cheshire "5.13.0"]
                 [org.apache.kafka/kafka-clients "3.7.0"]
                 [io.prometheus/prometheus-metrics-core "1.3.1"]
                 [io.prometheus/prometheus-metrics-exposition-formats "1.3.1"]
                 [org.clojure/core.async "1.7.701"]
                 [prismatic/schema "1.4.1"]
                 [org.clojure/tools.logging "1.3.0"]
                 [org.slf4j/slf4j-api "2.0.13"]
                 [ch.qos.logback/logback-classic "1.5.6"]]
  :source-paths ["src"]
  :test-paths ["test"]
  :resource-paths ["resources"]
  :target-path "target/%s/"
  :main odradek.core
  :jvm-opts ["-Xmx512m" "-Dclojure.compiler.direct-linking=true"]
  :profiles
  {:dev     {:dependencies [[nubank/state-flow "5.15.0"]
                            [nubank/matcher-combinators "3.9.1"]
                            [clj-http "3.13.0"]]
             :source-paths ["dev"]
             :repl-options {:init-ns user}}
   :test    {:resource-paths ["test-resources"]}
   :uberjar {:aot :all
             :jvm-opts ["-Dclojure.compiler.direct-linking=true"]}}
  :test-selectors {:default (constantly true)
                   :odradek.unit (fn [m] (re-find #"^odradek\.unit\." (str (:ns m))))
                   :odradek.integration (fn [m] (re-find #"^odradek\.integration\." (str (:ns m))))}
  :aliases {"test-all" ["do" "clean" ["test"]]})
