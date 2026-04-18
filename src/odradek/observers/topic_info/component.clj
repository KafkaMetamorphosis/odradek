(ns odradek.observers.topic-info.component
  (:require [clojure.core.async :as async]
            [clojure.tools.logging :as log]
            [com.stuartsierra.component :as component]
            [odradek.clients.kafka-admin :as kafka-admin]
            [odradek.observers.topic-info.logic :as topic-info-logic]
            [odradek.metrics.registry :as metrics]))

(defn- elapsed-seconds
  "Returns elapsed time in seconds since start-ns (a System/nanoTime value)."
  [start-ns]
  (/ (- (System/nanoTime) start-ns) 1e9))

(defn- build-admin-clients
  "Creates one AdminClient per cluster listed in the topic-info observer's clusters."
  [observer-clusters kafka-clusters]
  (into {}
        (for [cluster-name observer-clusters
              :let [cluster-config (get kafka-clusters cluster-name)]]
          [cluster-name (kafka-admin/new-admin-client (:bootstrap-url cluster-config))])))

(defn- init-scrape-error-counters!
  "Initializes topic scrape error counters to 0 for all clusters and steps.
   Ensures Grafana shows 0 rather than 'no data' when no errors have occurred."
  [cluster-names metrics-registry]
  (doseq [cluster-name cluster-names
          step         ["list" "describe" "describe-config"]]
    (metrics/init-topic-scrape-errors! metrics-registry cluster-name step)))

(defn- list-all-topics
  "Lists all non-internal topic names from the cluster via AdminClient.
   Returns [topic-names elapsed-seconds] on success.
   Returns nil and increments error counter on failure."
  [admin-client cluster-name metrics-registry]
  (let [list-start (System/nanoTime)]
    (try
      (let [topic-names      (kafka-admin/list-topics admin-client)
            list-duration    (elapsed-seconds list-start)]
        (metrics/observe-topic-list-duration! metrics-registry cluster-name list-duration)
        (log/debug "Listed topics" {:cluster cluster-name :topic-count (count topic-names)})
        topic-names)
      (catch Exception exception
        (metrics/inc-topic-scrape-errors! metrics-registry cluster-name "list")
        (log/warn exception "Failed to list topics" {:cluster cluster-name})
        nil))))

(defn- describe-all-topics
  "Describes all topics in the cluster via AdminClient.
   Returns topic-descriptions map on success.
   Returns nil and increments error counter on failure."
  [cluster-name topic-names admin-client metrics-registry]
  (let [describe-start (System/nanoTime)]
    (try
      (let [topic-descriptions    (kafka-admin/describe-topics admin-client topic-names)
            describe-duration     (elapsed-seconds describe-start)]
        (metrics/observe-topic-describe-duration! metrics-registry cluster-name describe-duration)
        (log/debug "Described topics" {:cluster cluster-name :topic-count (count topic-descriptions)})
        topic-descriptions)
      (catch Exception exception
        (metrics/inc-topic-scrape-errors! metrics-registry cluster-name "describe")
        (log/warn exception "Failed to describe topics" {:cluster cluster-name})
        nil))))

(defn- describe-all-topic-configs
  "Describes topic configs for all topics in the cluster via AdminClient.
   Returns topic-configs map on success.
   Returns nil and increments error counter on failure."
  [cluster-name topic-names admin-client metrics-registry]
  (let [config-describe-start (System/nanoTime)]
    (try
      (let [topic-configs             (kafka-admin/describe-topic-configs admin-client topic-names)
            config-describe-duration  (elapsed-seconds config-describe-start)]
        (metrics/observe-topic-config-describe-duration! metrics-registry cluster-name config-describe-duration)
        (log/debug "Described topic configs" {:cluster cluster-name :topic-count (count topic-configs)})
        topic-configs)
      (catch Exception exception
        (metrics/inc-topic-scrape-errors! metrics-registry cluster-name "describe-config")
        (log/warn exception "Failed to describe topic configs" {:cluster cluster-name})
        nil))))

(defn- export-topic-metrics!
  "Exports the kafka_odradek_topic_config info gauge for a single topic.
   Builds the full label-values map from the topic description and config,
   then calls set-topic-config! with the metrics-registry that holds the
   registered label order."
  [cluster-name topic-name topic-description topic-config {:keys [metrics-registry]}]
  (let [label-values-map (topic-info-logic/build-label-values
                           cluster-name topic-name topic-description topic-config)]
    (metrics/set-topic-config! metrics-registry label-values-map)))

(defn- scrape-cluster
  "Performs a full topic scrape for one cluster:
   1. Lists all topics
   2. Applies topics-filter-pattern to retain only matching topics
   3. Describes each matching topic
   4. Describes each matching topic's configs
   5. Exports per-topic gauges
   Times each step and records the total scrape duration."
  [cluster-name admin-client {:keys [metrics-registry observer] :as this}]
  (when-let [matched-topic-names (seq (->> (list-all-topics admin-client cluster-name metrics-registry)
                                           (filter #(re-matches (re-pattern (:topics-filter observer)) %))))]
    (let [topic-descriptions (describe-all-topics cluster-name matched-topic-names admin-client  metrics-registry)
          topic-configs      (describe-all-topic-configs cluster-name matched-topic-names admin-client metrics-registry)]
      (doseq [topic-name matched-topic-names
              :let [topic-description (get topic-descriptions topic-name)
                    topic-config      (get topic-configs topic-name)]
              :when (and topic-description topic-config)]
        (try
          (export-topic-metrics! cluster-name topic-name topic-description topic-config this)
          (catch Exception exception
            (log/warn exception "Failed to export metrics for topic"
                      {:cluster cluster-name :topic topic-name})))
        (log/debug "Exported topic metrics" {:cluster cluster-name
                                             :topic-count (count matched-topic-names)}))))
  (log/debug "Completed topic scrape cycle" {:cluster cluster-name}))

(defn- scrape-all-clusters
  "Scrapes all clusters for this observer. Runs on a real thread (blocking Kafka admin calls)."
  [observer admin-clients {:keys [metrics-registry] :as this}]

  ;; make sense in the future the cluster requests being made in parallel
  (doseq [[cluster-name admin-client] admin-clients
          :let [scrape-start (System/nanoTime)]]
    (try
      (scrape-cluster cluster-name admin-client this)
      (catch Exception exception
        (log/warn exception "Unexpected failure scraping cluster"
                  {:cluster  cluster-name
                   :observer (:name observer)})))
    (metrics/observe-topic-scrape-duration! metrics-registry cluster-name (elapsed-seconds scrape-start))))

(defrecord TopicScraperComponent [observer kafka-clusters metrics-registry trigger-ch admin-clients]
  component/Lifecycle
  (start [this]
    (let [observer-clusters         (:clusters observer)
          clients                   (build-admin-clients observer-clusters kafka-clusters)]
      (init-scrape-error-counters! observer-clusters metrics-registry)
      (async/go-loop []
        (when (async/<! trigger-ch)
          (async/<! (async/thread
                      (scrape-all-clusters observer clients this)))
          (recur)))
      (log/info "TopicScraper started" {:observer (:name observer)
                                        :clusters observer-clusters})
      (assoc this
             :admin-clients               clients)))

  (stop [this]
    (log/info "Stopping TopicScraper..." {:observer (:name observer)})
    ;; trigger-ch is closed by the orchestrator before stop is called — that exits the go-loop
    (doseq [[cluster-name admin-client] admin-clients]
      (try
        (kafka-admin/close-admin-client admin-client)
        (catch Exception exception
          (log/warn exception "Failed to close AdminClient" {:cluster cluster-name}))))
    (log/info "TopicScraper stopped." {:observer (:name observer)})
    (assoc this :admin-clients nil)))

(defn new-topic-info-observer [observer kafka-clusters metrics-registry trigger-ch]
  (map->TopicScraperComponent
   {:observer         observer
    :kafka-clusters   kafka-clusters
    :metrics-registry metrics-registry
    :trigger-ch       trigger-ch}))
