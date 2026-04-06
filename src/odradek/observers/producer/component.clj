(ns odradek.observers.producer.component
  (:require [clojure.core.async :as async]
            [clojure.tools.logging :as log]
            [com.stuartsierra.component :as component]
            [odradek.clients.kafka :as kafka]
            [odradek.observers.producer.logic :as producer-logic]
            [odradek.metrics.registry :as metrics])
  (:import [org.apache.kafka.clients.producer ProducerRecord]
           [org.apache.kafka.common.header.internals RecordHeader]
           [java.util UUID]))

(defn- make-producer-record
  "Builds a ProducerRecord with produced-at timestamp and message-id headers."
  [topic payload timestamp-ms]
  (let [record (ProducerRecord. topic nil payload)]
    (doto (.headers record)
      (.add (RecordHeader. "com.franz.odradek/produced-at" (.getBytes (str timestamp-ms) "UTF-8")))
      (.add (RecordHeader. "com.franz.odradek/message-id"  (.getBytes (str (UUID/randomUUID)) "UTF-8"))))
    record))

(defn- produce-message
  "Sends one encoded message to the topic. Records latency and counters."
  [kafka-producer topic message-size-kb labels metrics-registry]
  (let [timestamp-ms (System/currentTimeMillis)
        payload      (producer-logic/encode-payload timestamp-ms message-size-kb)
        record       (make-producer-record topic payload timestamp-ms)]
    (try
      (-> kafka-producer (.send record) .get)
      (metrics/inc-counter metrics-registry :produced-total labels)
      (metrics/observe-histogram metrics-registry :production-latency labels
                                 (- (System/currentTimeMillis) timestamp-ms))
      (catch Exception ex
        (log/error ex "Failed to produce message" {:observer     (:observer labels)
                                                   :cluster_name (:cluster_name labels)})
        (metrics/inc-counter metrics-registry :production-error-total labels)))))

(defn- produce-interval
  "Produces messages-per-interval messages to each cluster for this observer."
  [producers observer metrics-registry]
  (let [topic                 (:topic observer)
        message-size-kb       (get-in observer [:volume-config :message-size-kb])
        messages-per-interval (get-in observer [:volume-config :messages-per-interval])]
    (doseq [[cluster-name kafka-producer] producers]
      (let [labels (producer-logic/derive-labels observer cluster-name)]
        (dotimes [_ messages-per-interval]
          (produce-message kafka-producer topic message-size-kb labels metrics-registry))))))

(defn- build-producers
  "Creates one KafkaProducer per cluster listed in the observer's clusters."
  [observer kafka-clusters]
  (into {}
    (for [cluster-name (:clusters observer)
          :let [bootstrap-url (get-in kafka-clusters [cluster-name :bootstrap-url])]]
      [cluster-name (kafka/new-producer bootstrap-url (:producer-config observer))])))

(defrecord ProducerObserverComponent [observer kafka-clusters metrics-registry trigger-ch
                                      producers]
  component/Lifecycle
  (start [this]
    (let [built-producers (build-producers observer kafka-clusters)]
      (doseq [cluster-name (:clusters observer)]
        (metrics/init-production-error-labels metrics-registry
                                              (producer-logic/derive-labels observer cluster-name)))
      (async/go-loop []
        (when (async/<! trigger-ch)
          (async/<! (async/thread (produce-interval built-producers observer metrics-registry)))
          (recur)))
      (log/info "ProducerObserver started" {:observer (:name observer)
                                            :clusters (:clusters observer)
                                            :topic    (:topic observer)})
      (assoc this :producers built-producers)))

  (stop [this]
    (log/info "Stopping ProducerObserver..." {:observer (:name observer)})
    ;; trigger-ch is closed by the orchestrator before stop is called — that exits the go-loop
    (doseq [[cluster-name kafka-producer] producers]
      (try
        (.close kafka-producer)
        (catch Exception ex
          (log/warn ex "Failed to close KafkaProducer" {:cluster cluster-name}))))
    (log/info "ProducerObserver stopped." {:observer (:name observer)})
    (assoc this :producers nil)))

(defn new-producer-observer [observer kafka-clusters metrics-registry trigger-ch]
  (map->ProducerObserverComponent
    {:observer          observer
     :kafka-clusters    kafka-clusters
     :metrics-registry  metrics-registry
     :trigger-ch        trigger-ch}))
