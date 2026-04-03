(ns odradek.components.observer-engine
  (:require [clojure.core.async :as async]
            [clojure.tools.logging :as log]
            [com.stuartsierra.component :as component]
            [odradek.clients.kafka :as kafka-client]
            [odradek.logic.observer :as observer]
            [odradek.components.metrics-registry :as metrics])
  (:import [org.apache.kafka.clients.producer ProducerRecord]
           [org.apache.kafka.clients.consumer KafkaConsumer]
           [org.apache.kafka.common TopicPartition]
           [org.apache.kafka.common.errors WakeupException]
           [org.apache.kafka.common.header.internals RecordHeader]
           [java.time Duration]
           [java.util UUID]))

;;; ---- Helpers ----

(defn- drain-chan
  "Discards all buffered messages in ch without blocking."
  [ch]
  (loop []
    (when (some? (async/poll! ch))
      (recur))))

(defn- backoff-ms [attempts]
  (min (* 1000 (Math/pow 2 attempts)) 10000))

(defn- set-status! [observer-statuses key status]
  (swap! observer-statuses assoc key {:status status :since (System/currentTimeMillis)}))

(defn- make-producer-record [topic payload timestamp-ms]
  (let [message-id  (str (UUID/randomUUID))
        produced-at (str timestamp-ms)
        record      (ProducerRecord. topic nil payload)]
    (doto (.headers record)
      (.add (RecordHeader. "com.franz.odradek/produced-at" (.getBytes produced-at "UTF-8")))
      (.add (RecordHeader. "com.franz.odradek/message-id"  (.getBytes message-id  "UTF-8"))))
    record))

;;; ---- Producer thread ----

(defn- producer-loop
  "Runs on a dedicated Thread. Takes payloads from produce-ch, calls .send(),
   records metrics. Exits when produce-ch is closed."
  [kafka-producer produce-ch topic metrics-registry labels observer-statuses status-key]
  (loop [attempts 0]
    (when-let [payload (async/<!! produce-ch)]
      (let [next-attempts
            (try
              (let [send-start (System/currentTimeMillis)
                    ts-ms      send-start
                    record     (make-producer-record topic payload ts-ms)
                    future     (.send kafka-producer record)]
                (.get future)
                (let [latency (- (System/currentTimeMillis) send-start)]
                  (metrics/inc-counter metrics-registry :produced-total labels)
                  (metrics/observe-histogram metrics-registry :production-latency labels latency))
                (set-status! observer-statuses status-key :running)
                0)
              (catch Exception ex
                (log/error ex "Producer error" (select-keys labels [:observer :cluster_name]))
                (metrics/inc-counter metrics-registry :production-error-total labels)
                (set-status! observer-statuses status-key :backoff)
                (Thread/sleep (long (backoff-ms attempts)))
                (inc attempts)))]
        (recur next-attempts))))
  (set-status! observer-statuses status-key :stopped))

;;; ---- Consumer thread ----

(defn- seek-consumer-to-end!
  "Subscribes, polls once to trigger assignment, then seeks to end."
  [^KafkaConsumer consumer topic]
  (.subscribe consumer [topic])
  (.poll consumer (Duration/ofMillis 100))
  (let [partitions (.assignment consumer)]
    (.seekToEnd consumer partitions)
    ;; Force the seek to take effect by calling position on each partition
    (doseq [^TopicPartition partition partitions]
      (.position consumer partition))))

(defn- consumer-loop
  "Runs on a dedicated Thread. Polls Kafka, records metrics, commits.
   Exits on WakeupException (triggered by .wakeup() during stop)."
  [^KafkaConsumer consumer topic metrics-registry labels observer-statuses status-key]
  (try
    (seek-consumer-to-end! consumer topic)
    (set-status! observer-statuses status-key :running)
    (loop [attempts 0]
      (let [continue?
            (try
              (let [records (.poll consumer (Duration/ofMillis 500))
                    fetch-time (System/currentTimeMillis)]
                (when (pos? (.count records))
                  (metrics/observe-histogram metrics-registry :fetch-latency labels
                    (if-let [produced-at (some-> records .iterator .next
                                                 (observer/decode-produced-at-header))]
                      (- fetch-time produced-at)
                      0)))
                (doseq [record records]
                  (let [pre-commit-time  (System/currentTimeMillis)
                        produced-at      (observer/decode-produced-at-header record)]
                    (when produced-at
                      (metrics/observe-histogram metrics-registry :e2e-message-age labels
                        (- pre-commit-time produced-at)))
                    (metrics/inc-counter metrics-registry :fetched-total labels)
                    (.commitSync consumer)
                    (when produced-at
                      (metrics/observe-histogram metrics-registry :full-e2e labels
                        (- (System/currentTimeMillis) produced-at)))))
                (set-status! observer-statuses status-key :running)
                true)
              (catch WakeupException _
                false)
              (catch Exception ex
                (log/error ex "Consumer error" (select-keys labels [:observer :cluster_name]))
                (metrics/inc-counter metrics-registry :fetch-error-total labels)
                (set-status! observer-statuses status-key :backoff)
                (Thread/sleep (long (backoff-ms attempts)))
                true))]
        (when continue?
          (recur (if (= :backoff (:status (get @observer-statuses status-key))) (inc attempts) 0)))))
    (catch WakeupException _
      nil)
    (finally
      (set-status! observer-statuses status-key :stopped))))

;;; ---- Rate loop ----

(defn- rate-loop
  "go-loop: every rate-interval-ms, drains and refills each observer-cluster channel."
  [observer-cluster-pairs rate-interval-ms]
  (async/go-loop []
    (doseq [{:keys [channel observer]} observer-cluster-pairs]
      (drain-chan channel)
      (let [messages-per-bucket (:messages-per-bucket observer)
            message-size-kb     (:message-size-kb observer)]
        (dotimes [_ messages-per-bucket]
          (let [ts      (System/currentTimeMillis)
                payload (observer/encode-payload ts message-size-kb)]
            (async/>! channel payload)))))
    (async/<! (async/timeout rate-interval-ms))
    (recur)))

;;; ---- Component ----

(defn- start-producer-thread [kafka-producer produce-ch topic metrics-registry labels observer-statuses status-key]
  (doto (Thread.
          #(producer-loop kafka-producer produce-ch topic metrics-registry labels observer-statuses status-key))
    (.setName (str "odradek-producer-" (:observer labels) "-" (:cluster_name labels)))
    (.setDaemon true)
    .start))

(defn- start-consumer-thread [kafka-consumer topic metrics-registry labels observer-statuses status-key]
  (doto (Thread.
          #(consumer-loop kafka-consumer topic metrics-registry labels observer-statuses status-key))
    (.setName (str "odradek-consumer-" (:observer labels) "-" (:cluster_name labels)))
    (.setDaemon true)
    .start))

(defn- join-thread [^Thread thread timeout-ms]
  (.join thread timeout-ms)
  (when (.isAlive thread)
    (.interrupt thread)))

(defrecord ObserverEngineComponent [config metrics-registry
                                    observer-statuses channels
                                    producers consumers
                                    producer-threads consumer-threads
                                    rate-loop-ch]
  component/Lifecycle
  (start [this]
    (let [raw-config       (:config config)
          observers        (:observers raw-config)
          kafka-clusters   (:kafka_clusters raw-config)
          rate-interval-ms (get-in raw-config [:producer-engine :rate-interval-ms])
          obs-statuses     (atom {})
          all-channels     (atom {})
          all-producers    (atom {})
          all-consumers    (atom {})
          prod-threads     (atom [])
          cons-threads     (atom [])]
      (doseq [observer observers
              cluster-name (:clusters observer)]
        (let [cluster-config  (get kafka-clusters cluster-name)
              bootstrap-url   (:bootstrap-url cluster-config)
              topic           (:topic observer)
              parallelism     (:parallelism observer)
              labels          (observer/derive-labels observer cluster-name)
              produce-ch      (async/chan parallelism)
              producer        (kafka-client/new-producer bootstrap-url (:producer-config observer))
              consumer        (kafka-client/new-consumer bootstrap-url (:consumer-config observer) (:name observer))
              pair-key        [(:name observer) cluster-name]
              prod-status-key [(:name observer) cluster-name :producer]
              cons-status-key [(:name observer) cluster-name :consumer]]
          (set-status! obs-statuses prod-status-key :starting)
          (set-status! obs-statuses cons-status-key :starting)
          (swap! all-channels assoc pair-key produce-ch)
          (swap! all-producers assoc pair-key producer)
          (swap! all-consumers assoc pair-key consumer)
          (dotimes [_ parallelism]
            (swap! prod-threads conj
              (start-producer-thread producer produce-ch topic metrics-registry labels obs-statuses prod-status-key)))
          (swap! cons-threads conj
            (start-consumer-thread consumer topic metrics-registry labels obs-statuses cons-status-key))))
      (let [observer-cluster-pairs
            (for [observer observers
                  cluster-name (:clusters observer)]
              {:channel  (get @all-channels [(:name observer) cluster-name])
               :observer observer
               :cluster-name cluster-name})
            rate-ch (rate-loop (doall observer-cluster-pairs) rate-interval-ms)]
        (assoc this
          :observer-statuses obs-statuses
          :channels          @all-channels
          :producers         @all-producers
          :consumers         @all-consumers
          :producer-threads  @prod-threads
          :consumer-threads  @cons-threads
          :rate-loop-ch      rate-ch))))

  (stop [this]
    (log/info "Stopping ObserverEngine...")
    ;; 1. Close all produce channels -- unblocks producer threads blocked on take
    (doseq [ch (vals channels)]
      (async/close! ch))
    ;; 2. Wakeup all consumers -- interrupts blocking .poll
    (doseq [consumer (vals consumers)]
      (.wakeup consumer))
    ;; 3. Stop rate loop
    (when rate-loop-ch
      (async/close! rate-loop-ch))
    ;; 4. Join all threads
    (doseq [t (concat producer-threads consumer-threads)]
      (join-thread t 10000))
    ;; 5. Close Kafka clients
    (doseq [producer (vals producers)]
      (.close producer))
    (doseq [consumer (vals consumers)]
      (.close consumer))
    (log/info "ObserverEngine stopped.")
    (assoc this
      :observer-statuses nil
      :channels          nil
      :producers         nil
      :consumers         nil
      :producer-threads  nil
      :consumer-threads  nil
      :rate-loop-ch      nil)))

(defn new-observer-engine []
  (map->ObserverEngineComponent {}))
