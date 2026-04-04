(ns odradek.integration.flows.topic-scraper-test
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
  (flow "kafka_odradek_topic_config appears in /metrics after scraper runs"
    [system (state-flow.api/get-state)]
    [:let [port (-> system :http-server :server .getConnectors first .getLocalPort)
           found (test-kafka/wait-for-metric port "kafka_odradek_topic_config" 15000)]]
    (state-flow.api/return
      (is (true? found)))))

(defflow topic-config-info-contains-expected-labels
  {:init init-system :cleanup stop-system}
  (flow "kafka_odradek_topic_config contains topic and cluster labels"
    [system (state-flow.api/get-state)]
    [:let [port  (-> system :http-server :server .getConnectors first .getLocalPort)
           _     (test-kafka/wait-for-metric port "kafka_odradek_topic_config" 15000)
           resp  (clj-http.client/get (str "http://localhost:" port "/metrics")
                   {:throw-exceptions false :as :text})
           body  (:body resp)]]
    (state-flow.api/return
      (do
        (is (str/includes? body "kafka_odradek_topic_config"))
        (is (str/includes? body "ODRADEK-TEST-TOPIC"))
        (is (str/includes? body "test-cluster"))))))
