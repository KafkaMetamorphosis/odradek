(ns odradek.observers.consumer.component
  (:require [clojure.tools.logging :as log]
            [com.stuartsierra.component :as component]
            [odradek.clients.kafka :as kafka]
            [odradek.observers.consumer.logic :as consumer-logic]
            [odradek.metrics.registry :as metrics])
  (:import [java.time Duration]))

(defn- seek-to-end
  "Polls once to trigger partition assignment, then seeks all assigned
   partitions to the end so only post-startup messages are measured."
  [kafka-consumer topic poll-timeout]
  (.subscribe kafka-consumer [topic])
  (.poll kafka-consumer poll-timeout)
  (let [assigned (.assignment kafka-consumer)]
    (when (seq assigned)
      (.seekToEnd kafka-consumer assigned)
      (log/debug "Seeked to end" {:topic      topic
                                   :partitions (mapv #(.partition %) assigned)}))))

(defn- process-record
  "Processes a single ConsumerRecord: decodes the produced-at header,
   records fetch-latency and e2e-message-age metrics."
  [record fetch-time-ms labels metrics-registry]
  (when-let [produced-at-ms (consumer-logic/decode-produced-at-header record)]
    (let [message-age-ms (- fetch-time-ms produced-at-ms)]
      (metrics/observe-histogram metrics-registry :fetch-latency labels message-age-ms)
      (metrics/observe-histogram metrics-registry :e2e-message-age labels message-age-ms)
      (log/debug "Consumed message"
                 {:observer    (:observer labels)
                  :cluster     (:cluster_name labels)
                  :produced-at produced-at-ms
                  :fetch-time  fetch-time-ms
                  :age-ms      message-age-ms}))))

(defn- commit-and-record-e2e
  "Commits offsets synchronously and records the full end-to-end latency
   (produced-at to post-commit) for each record."
  [kafka-consumer records fetch-time-ms labels metrics-registry]
  (.commitSync kafka-consumer)
  (let [committed-at-ms (System/currentTimeMillis)]
    (doseq [record records]
      (when-let [produced-at-ms (consumer-logic/decode-produced-at-header record)]
        (let [full-e2e-ms (- committed-at-ms produced-at-ms)]
          (metrics/observe-histogram metrics-registry :full-e2e labels full-e2e-ms)
          (log/debug "Committed message"
                     {:observer     (:observer labels)
                      :cluster      (:cluster_name labels)
                      :produced-at  produced-at-ms
                      :committed-at committed-at-ms
                      :full-e2e-ms  full-e2e-ms}))))))

(defn- poll-loop
  "Blocking poll loop for one cluster. Polls continuously until stop-flag
   is set to true. Runs on a real thread via future."
  [kafka-consumer topic labels metrics-registry stop-flag]
  (let [poll-timeout (Duration/ofMillis 1000)]
    (seek-to-end kafka-consumer topic poll-timeout)
    (loop []
      (when-not @stop-flag
        (try
          (let [consumer-records (.poll kafka-consumer poll-timeout)
                fetch-time-ms    (System/currentTimeMillis)
                records          (seq consumer-records)]
            (when records
              (doseq [record records]
                (process-record record fetch-time-ms labels metrics-registry)
                (metrics/inc-counter metrics-registry :fetched-total labels))
              (commit-and-record-e2e kafka-consumer records fetch-time-ms labels metrics-registry)))
          (catch Exception exception
            (log/error exception "Consumer poll/commit failed"
                       {:observer (:observer labels)
                        :cluster  (:cluster_name labels)})
            (metrics/inc-counter metrics-registry :fetch-error-total labels)))
        (recur)))))

(defn- build-consumers
  "Creates one KafkaConsumer per cluster, subscribed to the observer's topic."
  [observer kafka-clusters]
  (let [group-id       (consumer-logic/observer-group-id (:name observer) (:consumer-config observer))
        consumer-config (assoc (:consumer-config observer) "group.id" group-id)]
    (into {}
      (for [cluster-name (:clusters observer)
            :let [bootstrap-url (get-in kafka-clusters [cluster-name :bootstrap-url])]]
        [cluster-name (kafka/new-consumer bootstrap-url consumer-config)]))))

(defn- start-consumer-futures
  "Starts a blocking poll-loop thread per cluster. Returns a map of
   cluster-name to a future (for joining on stop)."
  [consumers observer metrics-registry stop-flag]
  (into {}
    (for [[cluster-name kafka-consumer] consumers]
      (let [labels (consumer-logic/derive-labels observer cluster-name)]
        [cluster-name (future
                        (poll-loop kafka-consumer
                                   (:topic observer)
                                   labels
                                   metrics-registry
                                   stop-flag))]))))

(defrecord ConsumerObserverComponent [observer kafka-clusters metrics-registry
                                      consumers stop-flag poll-futures]
  component/Lifecycle
  (start [this]
    (let [built-consumers (build-consumers observer kafka-clusters)
          flag            (volatile! false)
          futures         (start-consumer-futures built-consumers observer metrics-registry flag)]
      (doseq [cluster-name (:clusters observer)]
        (metrics/init-fetch-error-labels metrics-registry
                                          (consumer-logic/derive-labels observer cluster-name)))
      (log/info "ConsumerObserver started" {:observer (:name observer)
                                            :clusters (:clusters observer)
                                            :topic    (:topic observer)})
      (assoc this
        :consumers    built-consumers
        :stop-flag    flag
        :poll-futures futures)))

  (stop [this]
    (log/info "Stopping ConsumerObserver..." {:observer (:name observer)})
    ;; Signal all poll-loops to exit
    (when stop-flag
      (vreset! stop-flag true))
    ;; Wait for each future to finish (with timeout)
    (doseq [[cluster-name poll-future] poll-futures]
      (let [result (deref poll-future 5000 :timeout)]
        (when (= result :timeout)
          (log/warn "Consumer thread did not stop within timeout" {:cluster cluster-name}))))
    ;; Close Kafka consumers
    (doseq [[cluster-name kafka-consumer] consumers]
      (try
        (.close kafka-consumer (Duration/ofSeconds 5))
        (catch Exception exception
          (log/warn exception "Failed to close KafkaConsumer" {:cluster cluster-name}))))
    (log/info "ConsumerObserver stopped." {:observer (:name observer)})
    (assoc this :consumers nil :stop-flag nil :poll-futures nil)))

(defn new-consumer-observer [observer kafka-clusters metrics-registry]
  (map->ConsumerObserverComponent
    {:observer         observer
     :kafka-clusters   kafka-clusters
     :metrics-registry metrics-registry}))
