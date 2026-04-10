(ns odradek.observers.orchestrator
  (:require [clojure.core.async :as async]
            [clojure.tools.logging :as log]
            [com.stuartsierra.component :as component]
            [odradek.observers.topic-info.component :as topic-info-component]
            [odradek.observers.producer.component :as producer-component]
            [odradek.observers.consumer.component :as consumer-component]))

;;; ---- Ticker ----

(defn- start-ticker
  "Starts a go-loop that fires `true` into each trigger channel every interval-ms.
   Returns a stop channel — close it to terminate the ticker."
  [interval-ms trigger-channels]
  (let [stop-channel (async/chan)]
    (async/go-loop []
      (async/<! (async/timeout interval-ms))
      (when-not (async/poll! stop-channel)
        (doseq [trigger-channel trigger-channels]
          (async/>! trigger-channel true))
        (recur)))
    stop-channel))

;;; ---- Observer construction ----

(defn- build-producer-observer
  "Creates a producer observer entry with a buffered trigger channel and component."
  [observer raw-config metrics-registry]
  (let [trigger-channel (async/chan)
        kafka-clusters  (:kafka_clusters raw-config)]
    {:trigger-ch trigger-channel
     :component  (component/start
                   (producer-component/new-producer-observer
                     observer kafka-clusters metrics-registry trigger-channel))}))

(defn- build-consumer-observer
  "Creates a consumer observer entry with no trigger channel.
   Consumer is self-driven (continuous poll loop), not ticker-driven."
  [observer raw-config metrics-registry]
  (let [kafka-clusters (:kafka_clusters raw-config)]
    {:trigger-ch nil
     :component  (component/start
                   (consumer-component/new-consumer-observer
                     observer kafka-clusters metrics-registry))}))

(defn- build-topic-info-observer
  "Creates a topic-info observer entry with a trigger channel and component."
  [observer raw-config metrics-registry]
  (let [trigger-channel (async/chan)
        kafka-clusters  (:kafka_clusters raw-config)]
    {:trigger-ch trigger-channel
     :component  (component/start
                   (topic-info-component/new-topic-info-observer
                     observer kafka-clusters metrics-registry trigger-channel))}))

(defn- build-observer-entry
  "Dispatches observer construction based on observer-type.
   Returns a map with :observer-type preserved for later grouping."
  [observer raw-config metrics-registry]
  (let [observer-type (:observer-type observer)]
    (case observer-type
      "producer"   (assoc (build-producer-observer observer raw-config metrics-registry) :observer-type observer-type)
      "consumer"   (assoc (build-consumer-observer observer raw-config metrics-registry) :observer-type observer-type)
      "topic-info" (assoc (build-topic-info-observer observer raw-config metrics-registry) :observer-type observer-type)
      (do (log/warn "Unknown observer type, skipping" {:observer-type observer-type
                                                       :name          (:name observer)})
          nil))))

(defn- collect-trigger-channels
  "Extracts non-nil trigger channels from built observers matching the given type."
  [built-observers observer-type]
  (->> (vals built-observers)
       (filter #(= observer-type (:observer-type %)))
       (keep :trigger-ch)
       vec))

;;; ---- Component ----

(defrecord ObserverOrchestratorComponent [config metrics-registry started-observers
                                          rate-loop-ch topic-info-loop-ch]
  component/Lifecycle
  (start [this]
    (log/info "Starting ObserverOrchestrator...")
    (let [raw-config              (get-in this [:config :config])
          observers               (:observers raw-config)
          rate-interval-ms        (get-in raw-config [:orchestrator-config :rate-interval-ms])
          built-observers         (reduce
                                    (fn [accumulator observer]
                                      (if-let [entry (build-observer-entry observer raw-config metrics-registry)]
                                        (assoc accumulator (:name observer) entry)
                                        accumulator))
                                    {}
                                    observers)
          producer-trigger-channels   (collect-trigger-channels built-observers "producer")
          topic-info-trigger-channels (collect-trigger-channels built-observers "topic-info")
          scrape-interval-ms      (get-in raw-config [:orchestrator-config :scrape-interval-ms])
          rate-stop-channel       (when (seq producer-trigger-channels)
                                    (log/info "Starting rate ticker" {:interval-ms    rate-interval-ms
                                                                      :producer-count (count producer-trigger-channels)})
                                    (start-ticker rate-interval-ms producer-trigger-channels))
          topic-info-stop-channel (when (seq topic-info-trigger-channels)
                                    (log/info "Starting topic-info ticker" {:interval-ms    scrape-interval-ms
                                                                            :observer-count (count topic-info-trigger-channels)})
                                    (start-ticker scrape-interval-ms topic-info-trigger-channels))]
      (log/info "ObserverOrchestrator started" {:observer-count (count built-observers)
                                                :observer-names (keys built-observers)})
      (assoc this
        :started-observers   built-observers
        :rate-loop-ch        rate-stop-channel
        :topic-info-loop-ch  topic-info-stop-channel)))

  (stop [this]
    (log/info "Stopping ObserverOrchestrator...")
    (when rate-loop-ch
      (log/info "Stopping rate ticker")
      (async/close! rate-loop-ch))
    (when topic-info-loop-ch
      (log/info "Stopping topic-info ticker")
      (async/close! topic-info-loop-ch))
    ;; Close trigger channels first — unblocks any component threads blocking on them
    (doseq [[_ {:keys [trigger-ch]}] started-observers]
      (when trigger-ch
        (async/close! trigger-ch)))
    ;; Now stop components — threads can exit cleanly since channels are already closed
    (doseq [[observer-name {:keys [component]}] started-observers]
      (log/info "Stopping observer" {:name observer-name})
      (when component
        (component/stop component)))
    (log/info "ObserverOrchestrator stopped.")
    (assoc this
      :observer-statuses  nil
      :started-observers  nil
      :rate-loop-ch       nil
      :topic-info-loop-ch nil)))

(defn new-observers-orchestrator []
  (map->ObserverOrchestratorComponent {}))
