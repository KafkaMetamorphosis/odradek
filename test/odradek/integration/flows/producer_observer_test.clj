(ns odradek.integration.flows.producer-observer-test
  (:require [clojure.test :refer [is]]
            [com.stuartsierra.component :as component]
            [state-flow.api :refer [defflow flow]]
            [odradek.helpers.system :as test-system]
            [odradek.helpers.kafka :as test-kafka]))

(defn- init-system-with-custom-labels [] (test-system/build-test-system-with-custom-labels))

(defn- init-system [] (test-system/build-test-system))
(defn- stop-system [system] (component/stop system))

(defn- server-port [system]
  (-> system :http-server :server .getConnectors first .getLocalPort))

(def ^:private expected-producer-labels
  {"cluster_name"              "test-cluster"
   "observer"                  "test-producer"
   "topic"                     "ODRADEK-TEST-TOPIC"
   "message_size_kb"           "1"
   "configured_rate_interval"  "1"})

(defn- assert-producer-labels [parsed-metric]
  (let [labels (:labels parsed-metric)]
    (doseq [[label-name expected-value] expected-producer-labels]
      (is (= expected-value (get labels label-name))
          (str "label " label-name " should be " expected-value)))))

(defflow produced-counter-has-correct-labels
  {:init init-system :cleanup stop-system}
  (flow "kafka_odradek_messages_produced_total has correct producer labels and value >= 1.0"
    [system (state-flow.api/get-state)]
    [:let [port  (server-port system)
           found (test-kafka/wait-for-metric port "kafka_odradek_messages_produced_total" 30000)
           _     (is (true? found) "produced_total metric must appear within 30s")
           body  (test-kafka/scrape-metrics port)
           parsed (test-kafka/parse-metric-line body "kafka_odradek_messages_produced_total")]]
    (state-flow.api/return
      (do
        (is (some? parsed) "produced_total metric line must be parseable")
        (assert-producer-labels parsed)
        (is (>= (:value parsed) 1.0) "produced counter must be >= 1.0")))))

(defflow production-latency-histogram-has-correct-labels
  {:init init-system :cleanup stop-system}
  (flow "kafka_odradek_messages_production_latency_ms_count has correct producer labels"
    [system (state-flow.api/get-state)]
    [:let [port  (server-port system)
           found (test-kafka/wait-for-metric port "kafka_odradek_messages_production_latency_ms_count" 30000)
           _     (is (true? found) "production_latency_ms_count must appear within 30s")
           body  (test-kafka/scrape-metrics port)
           parsed (test-kafka/parse-metric-line body "kafka_odradek_messages_production_latency_ms_count")]]
    (state-flow.api/return
      (do
        (is (some? parsed) "production_latency_ms_count line must be parseable")
        (assert-producer-labels parsed)
        (is (>= (:value parsed) 1.0) "histogram count must be >= 1.0")))))

(defflow production-error-counter-initialized-to-zero
  {:init init-system :cleanup stop-system}
  (flow "kafka_odradek_messages_production_error_total is initialized to 0.0"
    [system (state-flow.api/get-state)]
    [:let [port  (server-port system)
           ;; Wait for produced_total to confirm the produce cycle ran (and error counter was initialized)
           found (test-kafka/wait-for-metric port "kafka_odradek_messages_produced_total" 30000)
           _     (is (true? found) "produced_total must appear to confirm init ran")
           body  (test-kafka/scrape-metrics port)
           parsed (test-kafka/parse-metric-line body "kafka_odradek_messages_production_error_total")]]
    (state-flow.api/return
      (do
        (is (some? parsed) "production_error_total line must be present")
        (assert-producer-labels parsed)
        (is (== 0.0 (:value parsed)) "error counter must be 0.0")))))

(defflow produced-counter-value-increments-over-time
  {:init init-system :cleanup stop-system}
  (flow "kafka_odradek_messages_produced_total increments between two scrapes"
    [system (state-flow.api/get-state)]
    [:let [port   (server-port system)
           found  (test-kafka/wait-for-metric port "kafka_odradek_messages_produced_total" 30000)
           _      (is (true? found) "produced_total must appear within 30s")
           body1  (test-kafka/scrape-metrics port)
           value1 (:value (test-kafka/parse-metric-line body1 "kafka_odradek_messages_produced_total"))
           _      (Thread/sleep 500)
           body2  (test-kafka/scrape-metrics port)
           value2 (:value (test-kafka/parse-metric-line body2 "kafka_odradek_messages_produced_total"))]]
    (state-flow.api/return
      (is (> value2 value1)
          (str "counter must increment: " value2 " > " value1)))))

(defflow produced-counter-emits-custom-labels
  {:init init-system-with-custom-labels :cleanup stop-system}
  (flow "kafka_odradek_messages_produced_total includes custom label dimensions when observer defines them"
    [system (state-flow.api/get-state)]
    [:let [port  (server-port system)
           found (test-kafka/wait-for-metric port "kafka_odradek_messages_produced_total" 30000)
           _     (is (true? found) "produced_total metric must appear within 30s")
           body  (test-kafka/scrape-metrics port)
           parsed (test-kafka/parse-metric-line body "kafka_odradek_messages_produced_total")]]
    (state-flow.api/return
      (do
        (is (some? parsed) "produced_total metric line must be parseable")
        (is (= "20" (get (:labels parsed) "slo_latency_ms"))
            "custom label slo_latency_ms must appear with value \"20\"")
        (is (= "1" (get (:labels parsed) "slo_latency_window_minutes"))
            "custom label slo_latency_window_minutes must appear with value \"1\"")))))
