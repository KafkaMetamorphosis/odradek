(ns odradek.helpers.system
  (:require [com.stuartsierra.component :as component]
            [odradek.system :as system]))

(def test-config
  {:server          {:port 0}
   :producer-engine {:rate-interval-ms 100}
   :topic-scraper   {:scrape-interval-ms 5000}
   :kafka_clusters  {"test-cluster" {:bootstrap-url "localhost:9092"}}
   :observers       [{:name                "test-observer"
                      :clusters            ["test-cluster"]
                      :topic               "ODRADEK-TEST-TOPIC"
                      :parallelism         1
                      :messages-per-bucket 1
                      :message-size-kb     1
                      :producer-config     {"acks"                "all"
                                            "linger.ms"           "0"
                                            "retries"             "0"
                                            "delivery.timeout.ms" "5000"
                                            "request.timeout.ms"  "1500"}
                      :consumer-config     {}}]})

(defn build-test-system []
  (component/start (system/new-system-with-config test-config)))
