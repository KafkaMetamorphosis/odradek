(ns odradek.helpers.system
  (:require [com.stuartsierra.component :as component]
            [odradek.system :as system]))

(def kafka-bootstrap
  (or (System/getenv "KAFKA_BOOTSTRAP") "localhost:9092"))

(def test-config
  {:server              {:port 0}
   :orchestrator-config {:rate-interval-ms   100
                         :scrape-interval-ms 5000}
   :kafka_clusters      {"test-cluster" {:bootstrap-url kafka-bootstrap}}
   :observers           [{:name            "test-producer"
                          :clusters        ["test-cluster"]
                          :topic           "ODRADEK-TEST-TOPIC"
                          :observer-type   "producer"
                          :volume-config   {:parallelism         1
                                            :messages-per-interval 1
                                            :message-size-kb     1}
                          :producer-config {"acks"                "all"
                                            "linger.ms"           "0"
                                            "retries"             "0"
                                            "delivery.timeout.ms" "5000"
                                            "request.timeout.ms"  "1500"}}
                         {:name            "test-consumer"
                          :clusters        ["test-cluster"]
                          :topic           "ODRADEK-TEST-TOPIC"
                          :observer-type   "consumer"
                          :consumer-config {}}
                         {:name            "test-topic-info"
                          :clusters        ["test-cluster"]
                          :observer-type   "topic-info"}]})

(def test-config-with-custom-labels
  (update test-config :observers
          (fn [observers]
            (mapv (fn [observer]
                    (if (= (:name observer) "test-producer")
                      (assoc observer :custom-labels {:slo-latency-ms          20
                                                      :slo-latency-window-minutes 1})
                      observer))
                  observers))))

(defn build-test-system []
  (component/start (system/new-system-with-config test-config)))

(defn build-test-system-with-custom-labels []
  (component/start (system/new-system-with-config test-config-with-custom-labels)))
