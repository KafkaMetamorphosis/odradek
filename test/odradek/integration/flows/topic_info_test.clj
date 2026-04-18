(ns odradek.integration.flows.topic-info-test
  (:require [clojure.test :refer [is]]
            [clojure.string :as str]
            [com.stuartsierra.component :as component]
            [state-flow.api :refer [defflow flow]]
            [odradek.helpers.system :as test-system]
            [odradek.helpers.kafka :as test-kafka]
            [odradek.system :as system]))

(defn- init-system [] (test-system/build-test-system))
(defn- stop-system [system] (component/stop system))

;; ---------------------------------------------------------------------------
;; Legacy info-gauge: kafka_odradek_topic_config
;; ---------------------------------------------------------------------------

(defflow topic-config-info-appears-in-metrics
  {:init init-system :cleanup stop-system}
  (flow "kafka_odradek_topic_config appears in /metrics after topic-info observer runs"
    [system (state-flow.api/get-state)]
    [:let [port (-> system :http-server :server .getConnectors first .getLocalPort)
           ;; Use the opening brace to distinguish the legacy gauge from
           ;; kafka_odradek_topic_config_<key> numeric config gauges
           found (test-kafka/wait-for-metric port "kafka_odradek_topic_config{" 15000)]]
    (state-flow.api/return
      (is (true? found)))))

(defn- parse-topic-config-labels
  "Finds the kafka_odradek_topic_config line for the given topic in Prometheus
   exposition text and extracts label key=value pairs into a map."
  [body topic-name]
  (let [metric-line (->> (str/split-lines body)
                         (filter #(and (str/starts-with? % "kafka_odradek_topic_config{")
                                       (str/includes? % topic-name)))
                         first)]
    (when metric-line
      (let [labels-str (second (re-find #"\{(.+)\}" metric-line))]
        (into {}
          (map (fn [pair]
                 (let [[label-key label-value] (str/split pair #"=" 2)]
                   [label-key (str/replace label-value #"\"" "")]))
               (re-seq #"[a-z_]+=(?:\"[^\"]*\")" labels-str)))))))

(defflow topic-config-info-contains-expected-labels
  {:init init-system :cleanup stop-system}
  (flow "kafka_odradek_topic_config contains topic and cluster labels with correct values"
    [system (state-flow.api/get-state)]
    [:let [port  (-> system :http-server :server .getConnectors first .getLocalPort)
           ;; Wait for the legacy info gauge line specifically (with opening brace)
           ;; to avoid a false-positive match against kafka_odradek_topic_config_retention_ms
           _     (test-kafka/wait-for-metric port "kafka_odradek_topic_config{" 15000)
           resp  (clj-http.client/get (str "http://localhost:" port "/metrics")
                   {:throw-exceptions false :as :text})
           body    (:body resp)
           labels  (parse-topic-config-labels body "ODRADEK-TEST-TOPIC")]]
    (state-flow.api/return
      (do
        (is (some? labels) "metric line for ODRADEK-TEST-TOPIC must be present")
        (is (= "test-cluster" (get labels "cluster_name")))
        (is (= "ODRADEK-TEST-TOPIC" (get labels "topic")))
        (is (= "1" (get labels "partitions")))
        ;; cleanup_policy and retention_ms are in the test observe-configs list
        (is (= "delete" (get labels "cleanup_policy")))
        (is (some? (get labels "retention_ms")))))))

;; ---------------------------------------------------------------------------
;; Partition count appears as a label on kafka_odradek_topic_config
;; ---------------------------------------------------------------------------

(defflow topic-config-partitions-label-appears-in-metrics
  {:init init-system :cleanup stop-system}
  (flow "kafka_odradek_topic_config appears in /metrics with partitions label for ODRADEK-TEST-TOPIC"
    [system (state-flow.api/get-state)]
    [:let [port  (-> system :http-server :server .getConnectors first .getLocalPort)
           found (test-kafka/wait-for-metric port "kafka_odradek_topic_config{" 15000)]]
    (state-flow.api/return
      (is (true? found)))))

(defflow topic-config-partitions-label-has-correct-value
  {:init init-system :cleanup stop-system}
  (flow "kafka_odradek_topic_config has partitions=1 label for ODRADEK-TEST-TOPIC and value 1.0"
    [system (state-flow.api/get-state)]
    [:let [port   (-> system :http-server :server .getConnectors first .getLocalPort)
           _      (test-kafka/wait-for-metric port "kafka_odradek_topic_config{" 15000)
           body   (test-kafka/scrape-metrics port)
           parsed (test-kafka/parse-metric-line body "kafka_odradek_topic_config" "ODRADEK-TEST-TOPIC")]]
    (state-flow.api/return
      (do
        (is (some? parsed) "kafka_odradek_topic_config line for ODRADEK-TEST-TOPIC must be present")
        (is (= "test-cluster" (get (:labels parsed) "cluster_name")))
        (is (= "ODRADEK-TEST-TOPIC" (get (:labels parsed) "topic")))
        (is (= "1" (get (:labels parsed) "partitions")))
        (is (= 1.0 (:value parsed)))))))

(defflow topic-scrape-process-metrics-appear-in-metrics
  {:init init-system :cleanup stop-system}
  (flow "topic scrape process histograms and error counter appear in /metrics"
    [system (state-flow.api/get-state)]
    [:let [port  (-> system :http-server :server .getConnectors first .getLocalPort)
           _     (test-kafka/wait-for-metric port "kafka_odradek_topic_scrape_duration_seconds" 15000)
           body  (test-kafka/scrape-metrics port)]]
    (state-flow.api/return
      (do
        (is (str/includes? body "kafka_odradek_topic_scrape_duration_seconds"))
        (is (str/includes? body "kafka_odradek_topic_list_duration_seconds"))
        (is (str/includes? body "kafka_odradek_topic_describe_duration_seconds"))
        (is (str/includes? body "kafka_odradek_topic_config_describe_duration_seconds"))
        (is (str/includes? body "kafka_odradek_topic_scrape_errors_total"))))))

(defflow topic-scrape-errors-initialized-to-zero
  {:init init-system :cleanup stop-system}
  (flow "kafka_odradek_topic_scrape_errors_total is initialized to 0 for all steps"
    [system (state-flow.api/get-state)]
    [:let [port  (-> system :http-server :server .getConnectors first .getLocalPort)
           _     (test-kafka/wait-for-metric port "kafka_odradek_topic_scrape_errors_total" 15000)
           body  (test-kafka/scrape-metrics port)
           list-parsed         (test-kafka/parse-metric-line body "kafka_odradek_topic_scrape_errors_total" "\"list\"")
           describe-parsed     (test-kafka/parse-metric-line body "kafka_odradek_topic_scrape_errors_total" "\"describe\"")]]
    (state-flow.api/return
      (do
        (is (some? list-parsed)     "list step error counter must be present")
        (is (= 0.0 (:value list-parsed)) "list error counter must start at 0")
        (is (some? describe-parsed) "describe step error counter must be present")
        (is (= 0.0 (:value describe-parsed)) "describe error counter must start at 0")))))

;; ---------------------------------------------------------------------------
;; topics-filter: matching topic appears on kafka_odradek_topic_config with observe-config labels
;; ---------------------------------------------------------------------------

(defflow matching-topic-appears-on-topic-config-gauge
  {:init init-system :cleanup stop-system}
  (flow "kafka_odradek_topic_config appears for ODRADEK-TEST-TOPIC when topics-filter is '.*'"
    [system (state-flow.api/get-state)]
    [:let [port   (-> system :http-server :server .getConnectors first .getLocalPort)
           found  (test-kafka/wait-for-metric port "kafka_odradek_topic_config{" 15000)
           body   (test-kafka/scrape-metrics port)
           parsed (test-kafka/parse-metric-line body "kafka_odradek_topic_config" "ODRADEK-TEST-TOPIC")]]
    (state-flow.api/return
      (do
        (is (true? found) "kafka_odradek_topic_config must appear in /metrics")
        (is (some? parsed) "metric line for ODRADEK-TEST-TOPIC must be present")
        (is (= "test-cluster" (get (:labels parsed) "cluster_name")))
        (is (= "ODRADEK-TEST-TOPIC" (get (:labels parsed) "topic")))
        ;; retention_ms is in the test observe-configs list, so it appears as a label
        (is (some? (get (:labels parsed) "retention_ms")))))))

;; ---------------------------------------------------------------------------
;; topics-filter: non-matching topic does not appear on kafka_odradek_topic_config
;; ---------------------------------------------------------------------------

(defflow non-matching-topic-produces-no-metrics
  {:init (fn [] (component/start
                  (system/new-system-with-config
                    (assoc-in test-system/test-config
                              [:observers 2 :topics-filter]
                              "NO-MATCH-PREFIX-.*"))))
   :cleanup stop-system}
  (flow "a topic not matching topics-filter does not appear on kafka_odradek_topic_config"
    [system (state-flow.api/get-state)]
    [:let [port  (-> system :http-server :server .getConnectors first .getLocalPort)
           _     (test-kafka/wait-for-metric port "kafka_odradek_topic_scrape_duration_seconds" 15000)
           body  (test-kafka/scrape-metrics port)
           ;; ODRADEK-TEST-TOPIC must not appear on the info gauge when filter does not match
           topic-config-parsed (test-kafka/parse-metric-line body
                                 "kafka_odradek_topic_config" "ODRADEK-TEST-TOPIC")]]
    (state-flow.api/return
      (is (nil? topic-config-parsed)
          "kafka_odradek_topic_config must not appear for non-matching topic"))))

;; ---------------------------------------------------------------------------
;; observe-configs: config key appears as label on kafka_odradek_topic_config
;; ---------------------------------------------------------------------------

(defflow observe-config-appears-as-label-on-topic-config-gauge
  {:init init-system :cleanup stop-system}
  (flow "cleanup.policy appears as cleanup_policy label on kafka_odradek_topic_config for ODRADEK-TEST-TOPIC"
    [system (state-flow.api/get-state)]
    [:let [port   (-> system :http-server :server .getConnectors first .getLocalPort)
           found  (test-kafka/wait-for-metric port "kafka_odradek_topic_config{" 15000)
           body   (test-kafka/scrape-metrics port)
           parsed (test-kafka/parse-metric-line body "kafka_odradek_topic_config" "ODRADEK-TEST-TOPIC")]]
    (state-flow.api/return
      (do
        (is (true? found) "kafka_odradek_topic_config must appear in /metrics")
        (is (some? parsed) "metric line for ODRADEK-TEST-TOPIC must be present")
        (is (= "test-cluster" (get (:labels parsed) "cluster_name")))
        (is (= "ODRADEK-TEST-TOPIC" (get (:labels parsed) "topic")))
        (is (= "delete" (get (:labels parsed) "cleanup_policy"))
            "cleanup.policy must appear as cleanup_policy label with value 'delete'")))))
