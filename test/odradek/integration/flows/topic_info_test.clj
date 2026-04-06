(ns odradek.integration.flows.topic-info-test
  (:require [clojure.test :refer [is]]
            [clojure.string :as str]
            [com.stuartsierra.component :as component]
            [state-flow.api :refer [defflow flow]]
            [odradek.helpers.system :as test-system]
            [odradek.helpers.kafka :as test-kafka]))

(defn- init-system [] (test-system/build-test-system))
(defn- stop-system [system] (component/stop system))

(defflow topic-config-info-appears-in-metrics
  {:init init-system :cleanup stop-system}
  (flow "kafka_odradek_topic_config appears in /metrics after topic-info observer runs"
    [system (state-flow.api/get-state)]
    [:let [port (-> system :http-server :server .getConnectors first .getLocalPort)
           found (test-kafka/wait-for-metric port "kafka_odradek_topic_config" 15000)]]
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
           _     (test-kafka/wait-for-metric port "kafka_odradek_topic_config" 15000)
           resp  (clj-http.client/get (str "http://localhost:" port "/metrics")
                   {:throw-exceptions false :as :text})
           body    (:body resp)
           labels  (parse-topic-config-labels body "ODRADEK-TEST-TOPIC")]]
    (state-flow.api/return
      (do
        (is (some? labels) "metric line for ODRADEK-TEST-TOPIC must be present")
        (is (= "test-cluster" (get labels "cluster_name")))
        (is (= "ODRADEK-TEST-TOPIC" (get labels "topic")))
        (is (= "delete" (get labels "cleanup_policy")))
        (is (= "producer" (get labels "compression_type")))
        (is (= "1" (get labels "min_insync_replicas")))
        (is (= "1" (get labels "partitions")))))))
