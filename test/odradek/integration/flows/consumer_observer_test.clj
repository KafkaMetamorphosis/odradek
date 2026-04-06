(ns odradek.integration.flows.consumer-observer-test
  (:require [clojure.test :refer [is]]
            [com.stuartsierra.component :as component]
            [state-flow.api :refer [defflow flow]]
            [odradek.helpers.system :as test-system]
            [odradek.helpers.kafka :as test-kafka]))

(defn- init-system [] (test-system/build-test-system))
(defn- stop-system [system] (component/stop system))

(defn- server-port [system]
  (-> system :http-server :server .getConnectors first .getLocalPort))

(def ^:private expected-consumer-labels
  {"cluster_name"              "test-cluster"
   "observer"                  "test-consumer"
   "topic"                     "ODRADEK-TEST-TOPIC"
   "message_size_kb"           ""
   "configured_rate_interval"  ""})

(defn- assert-consumer-labels [parsed-metric]
  (let [labels (:labels parsed-metric)]
    (doseq [[label-name expected-value] expected-consumer-labels]
      (is (= expected-value (get labels label-name))
          (str "label " label-name " should be " expected-value)))))

(defflow fetched-counter-has-correct-labels
  {:init init-system :cleanup stop-system}
  (flow "kafka_odradek_messages_fetched_total has correct consumer labels and value >= 1.0"
    [system (state-flow.api/get-state)]
    [:let [port  (server-port system)
           found (test-kafka/wait-for-metric port "kafka_odradek_messages_fetched_total" 60000)
           _     (is (true? found) "fetched_total metric must appear within 60s")
           body  (test-kafka/scrape-metrics port)
           parsed (test-kafka/parse-metric-line body "kafka_odradek_messages_fetched_total")]]
    (state-flow.api/return
      (do
        (is (some? parsed) "fetched_total metric line must be parseable")
        (assert-consumer-labels parsed)
        (is (>= (:value parsed) 1.0) "fetched counter must be >= 1.0")))))

(defflow fetch-latency-histogram-has-correct-labels
  {:init init-system :cleanup stop-system}
  (flow "kafka_odradek_messages_fetch_latency_ms_count has correct consumer labels"
    [system (state-flow.api/get-state)]
    [:let [port  (server-port system)
           found (test-kafka/wait-for-metric port "kafka_odradek_messages_fetch_latency_ms_count" 60000)
           _     (is (true? found) "fetch_latency_ms_count must appear within 60s")
           body  (test-kafka/scrape-metrics port)
           parsed (test-kafka/parse-metric-line body "kafka_odradek_messages_fetch_latency_ms_count")]]
    (state-flow.api/return
      (do
        (is (some? parsed) "fetch_latency_ms_count line must be parseable")
        (assert-consumer-labels parsed)
        (is (>= (:value parsed) 1.0) "histogram count must be >= 1.0")))))

(defflow e2e-message-age-histogram-has-correct-labels
  {:init init-system :cleanup stop-system}
  (flow "kafka_odradek_e2e_message_age_ms_count has correct consumer labels"
    [system (state-flow.api/get-state)]
    [:let [port  (server-port system)
           found (test-kafka/wait-for-metric port "kafka_odradek_e2e_message_age_ms_count" 60000)
           _     (is (true? found) "e2e_message_age_ms_count must appear within 60s")
           body  (test-kafka/scrape-metrics port)
           parsed (test-kafka/parse-metric-line body "kafka_odradek_e2e_message_age_ms_count")]]
    (state-flow.api/return
      (do
        (is (some? parsed) "e2e_message_age_ms_count line must be parseable")
        (assert-consumer-labels parsed)
        (is (>= (:value parsed) 1.0) "histogram count must be >= 1.0")))))

(defflow full-e2e-histogram-has-correct-labels
  {:init init-system :cleanup stop-system}
  (flow "kafka_odradek_full_e2e_ms_count has correct consumer labels"
    [system (state-flow.api/get-state)]
    [:let [port  (server-port system)
           found (test-kafka/wait-for-metric port "kafka_odradek_full_e2e_ms_count" 60000)
           _     (is (true? found) "full_e2e_ms_count must appear within 60s")
           body  (test-kafka/scrape-metrics port)
           parsed (test-kafka/parse-metric-line body "kafka_odradek_full_e2e_ms_count")]]
    (state-flow.api/return
      (do
        (is (some? parsed) "full_e2e_ms_count line must be parseable")
        (assert-consumer-labels parsed)
        (is (>= (:value parsed) 1.0) "histogram count must be >= 1.0")))))

(defflow fetch-error-counter-initialized-to-zero
  {:init init-system :cleanup stop-system}
  (flow "kafka_odradek_messages_fetch_error_total is initialized to 0.0"
    [system (state-flow.api/get-state)]
    [:let [port  (server-port system)
           ;; Wait for fetched_total to confirm the consumer has polled successfully
           found (test-kafka/wait-for-metric port "kafka_odradek_messages_fetched_total" 60000)
           _     (is (true? found) "fetched_total must appear to confirm consumer ran")
           body  (test-kafka/scrape-metrics port)
           parsed (test-kafka/parse-metric-line body "kafka_odradek_messages_fetch_error_total")]]
    (state-flow.api/return
      (do
        (is (some? parsed) "fetch_error_total line must be present")
        (assert-consumer-labels parsed)
        (is (== 0.0 (:value parsed)) "error counter must be 0.0")))))

(defflow consumer-histogram-counts-match-fetched-counter
  {:init init-system :cleanup stop-system}
  (flow "fetch_latency, e2e_message_age, and full_e2e histogram counts all equal fetched_total"
    [system (state-flow.api/get-state)]
    [:let [port  (server-port system)
           found (test-kafka/wait-for-metric port "kafka_odradek_messages_fetched_total" 60000)
           _     (is (true? found) "fetched_total must appear within 60s")
           body  (test-kafka/scrape-metrics port)
           fetched-total     (:value (test-kafka/parse-metric-line body "kafka_odradek_messages_fetched_total"))
           fetch-latency-count (:value (test-kafka/parse-metric-line body "kafka_odradek_messages_fetch_latency_ms_count"))
           e2e-age-count     (:value (test-kafka/parse-metric-line body "kafka_odradek_e2e_message_age_ms_count"))
           full-e2e-count    (:value (test-kafka/parse-metric-line body "kafka_odradek_full_e2e_ms_count"))]]
    (state-flow.api/return
      (do
        (is (== fetch-latency-count fetched-total)
            (str "fetch_latency count (" fetch-latency-count ") must equal fetched_total (" fetched-total ")"))
        (is (== e2e-age-count fetched-total)
            (str "e2e_message_age count (" e2e-age-count ") must equal fetched_total (" fetched-total ")"))
        (is (== full-e2e-count fetched-total)
            (str "full_e2e count (" full-e2e-count ") must equal fetched_total (" fetched-total ")"))))))
