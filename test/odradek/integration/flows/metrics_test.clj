(ns odradek.integration.flows.metrics-test
  (:require [clojure.test :refer [is]]
            [clojure.string :as str]
            [state-flow.api :refer [defflow flow]]
            [odradek.helpers.system :as test-system]
            [odradek.helpers.kafka :as test-kafka]))

(defn- init-system []
  (test-system/build-test-system))

(defn- server-port [system]
  (-> system :http-server :server .getConnectors first .getLocalPort))

(defflow metrics-endpoint-returns-text-plain
  {:init init-system}
  (flow "GET /metrics returns 200 with text/plain content-type"
    [system (state-flow.api/get-state)]
    [:let [port    (server-port system)
           resp    (clj-http.client/get (str "http://localhost:" port "/metrics")
                     {:throw-exceptions false :as :text})]]
    (state-flow.api/return
      (do
        (is (= 200 (:status resp)))
        (is (str/includes? (get-in resp [:headers "Content-Type"]) "text/plain"))))))

(defflow produced-counter-appears-in-metrics
  {:init init-system}
  (flow "kafka_odradek_messages_produced_total appears after engine produces"
    [system (state-flow.api/get-state)]
    [:let [port  (server-port system)
           found (test-kafka/wait-for-metric port "kafka_odradek_messages_produced_total" 30000)]]
    (state-flow.api/return
      (is (true? found)))))

(defflow fetched-counter-appears-in-metrics
  {:init init-system}
  (flow "kafka_odradek_messages_fetched_total appears after consumer fetches"
    [system (state-flow.api/get-state)]
    [:let [port  (server-port system)
           found (test-kafka/wait-for-metric port "kafka_odradek_messages_fetched_total" 60000)]]
    (state-flow.api/return
      (is (true? found)))))
