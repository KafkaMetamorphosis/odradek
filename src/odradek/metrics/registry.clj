(ns odradek.metrics.registry
  (:require [com.stuartsierra.component :as component]
            [clojure.string :as str]
            [odradek.observers.topic-info.logic :as topic-info-logic])
  (:import [io.prometheus.metrics.core.metrics Counter Gauge Histogram]
           [io.prometheus.metrics.model.registry PrometheusRegistry]
           [io.prometheus.metrics.expositionformats ExpositionFormats]
           [java.io ByteArrayOutputStream]))

(def ^:private standard-label-names
  ["cluster_name" "observer" "topic" "message_size_kb" "configured_rate_interval"])

(def ^:private topic-config-label-names
  (into-array String ["cluster_name" "topic" "partitions" "replication_factor"
                      "partitions_replicas_broker_ids" "partitions_isr_broker_ids"
                      "partitions_leader_broker_ids" "partitions_replicas_broker_racks"
                      "min_insync_replicas" "retention_ms" "retention_bytes"
                      "cleanup_policy" "max_message_bytes" "compression_type"]))

(def ^:private topic-info-cluster-label-names
  (into-array String ["cluster_name"]))

(def ^:private topic-info-cluster-step-label-names
  (into-array String ["cluster_name" "step"]))

(def ^:private topic-info-topic-label-names
  (into-array String ["cluster_name" "topic"]))

(def ^:private topic-scrape-histogram-buckets
  (double-array [0.001 0.005 0.01 0.025 0.05 0.1 0.25 0.5 1.0 2.5 5.0 10.0 30.0 60.0]))

(def ^:private histogram-buckets
  (double-array [5 10 15 25 30 40 50 80 100 250 300 500 800 1000
                 1500 2000 3000 4000 5000 10000 15000 20000 25000 30000]))

(defn- sanitize-metric-name-segment
  "Replaces dots and dashes with underscores for use in a Prometheus metric name segment."
  [raw-segment]
  (str/replace raw-segment #"[\.\-]" "_"))

(defn- sanitize-label-name
  "Replaces any character not in [a-zA-Z0-9_] with underscore.
   Prometheus label names must match [a-zA-Z_][a-zA-Z0-9_]*."
  [raw-label-name]
  (str/replace (name raw-label-name) #"[^a-zA-Z0-9_]" "_"))

(defn- stringify-custom-labels
  "Converts a raw custom-labels map (e.g. {:slo-latency-ms 20}) to a sorted-map
   of sanitized string keys to string values (e.g. {\"slo_latency_ms\" \"20\"}).
   Returns an empty sorted-map when custom-labels is nil."
  [raw-custom-labels]
  (if (seq raw-custom-labels)
    (into (sorted-map)
          (map (fn [[label-key label-value]]
                 [(sanitize-label-name label-key) (str label-value)])
               raw-custom-labels))
    (sorted-map)))

(defn- compute-union-custom-label-keys
  "Returns a sorted vector of sanitized Prometheus label name strings
   representing the union of all custom-label keys across all observers.
   Observers without :custom-labels contribute nothing."
  [observers]
  (->> observers
       (mapcat (fn [observer]
                 (keys (:custom-labels observer))))
       (map sanitize-label-name)
       set
       sort
       vec))

(defn- build-label-names-array
  "Appends the union custom label key strings to the standard label names
   and returns a String array suitable for Prometheus metric registration."
  [union-custom-label-keys]
  (into-array String (concat standard-label-names union-custom-label-keys)))

(defn- register-counter [registry metric-name help label-names-arr]
  (-> (Counter/builder)
      (.name metric-name)
      (.help help)
      (.labelNames label-names-arr)
      (.register registry)))

(defn- register-gauge [registry metric-name help label-names-arr]
  (-> (Gauge/builder)
      (.name metric-name)
      (.help help)
      (.labelNames label-names-arr)
      (.register registry)))

(defn- register-histogram [registry metric-name help label-names-arr]
  (-> (Histogram/builder)
      (.name metric-name)
      (.help help)
      (.labelNames label-names-arr)
      (.classicUpperBounds histogram-buckets)
      (.register registry)))

(defn- register-topic-scrape-histogram [registry metric-name help label-names-arr]
  (-> (Histogram/builder)
      (.name metric-name)
      (.help help)
      (.labelNames label-names-arr)
      (.classicUpperBounds topic-scrape-histogram-buckets)
      (.register registry)))

(defn- numeric-config-metric-name
  "Returns the Prometheus gauge metric name for a numeric config key.
   Example: 'retention.ms' -> 'kafka_odradek_topic_config_retention_ms'"
  [config-key]
  (str "kafka_odradek_topic_config_" (sanitize-metric-name-segment config-key)))

(defn- register-numeric-config-gauges
  "Registers one Gauge per numeric config key in the observe-configs classification.
   Returns a map of original config key string to registered Gauge."
  [registry numeric-config-key-list]
  (into {}
    (map (fn [config-key]
           [config-key
            (register-gauge registry
                            (numeric-config-metric-name config-key)
                            (str config-key " config value for the topic")
                            topic-info-topic-label-names)])
         numeric-config-key-list)))

(defn- register-topic-string-config-gauge
  "Registers the kafka_odradek_topic_string_config gauge with cluster_name, topic, and
   one label per string config key. Returns the registered Gauge, or nil if
   string-config-key-list is empty."
  [registry string-config-key-list]
  (when (seq string-config-key-list)
    (let [sanitized-string-label-names (mapv sanitize-metric-name-segment string-config-key-list)
          label-names-arr              (into-array String
                                         (concat ["cluster_name" "topic"]
                                                 sanitized-string-label-names))]
      (register-gauge registry
                      "kafka_odradek_topic_string_config"
                      "Effective string topic configuration values as labels"
                      label-names-arr))))

(defn- find-topic-info-observer
  "Returns the first observer map with observer-type 'topic-info', or nil."
  [observers]
  (first (filter #(= "topic-info" (:observer-type %)) observers)))

(defrecord MetricsRegistryComponent [config registry metrics custom-label-keys
                                     numeric-config-keys string-config-keys]
  component/Lifecycle
  (start [this]
    (let [observers                (get-in this [:config :config :observers])
          union-custom-label-keys  (compute-union-custom-label-keys observers)
          label-names-arr          (build-label-names-array union-custom-label-keys)
          topic-info-observer      (find-topic-info-observer observers)
          observe-configs          (get topic-info-observer :observe-configs [])
          configs-classification   (topic-info-logic/classify-observe-configs observe-configs)
          numeric-config-key-list  (:numeric configs-classification)
          string-config-key-list   (:string configs-classification)
          reg                      (PrometheusRegistry.)
          numeric-config-gauges    (register-numeric-config-gauges reg numeric-config-key-list)
          topic-info-gauge         (register-topic-string-config-gauge reg string-config-key-list)
          base-metrics
            {:produced-total              (register-counter   reg "kafka_odradek_messages_produced_total"                 "Total messages produced"                                         label-names-arr)
             :production-error-total      (register-counter   reg "kafka_odradek_messages_production_error_total"            "Total production errors"                                         label-names-arr)
             :fetched-total               (register-counter   reg "kafka_odradek_messages_fetched_total"                     "Total messages fetched"                                          label-names-arr)
             :fetch-error-total           (register-counter   reg "kafka_odradek_messages_fetch_error_total"                 "Total fetch errors"                                              label-names-arr)
             :production-latency          (register-histogram reg "kafka_odradek_messages_production_latency_ms"             "Production ack latency ms"                                       label-names-arr)
             :fetch-latency               (register-histogram reg "kafka_odradek_messages_fetch_latency_ms"                  "Message fetch latency ms"                                        label-names-arr)
             :e2e-message-age             (register-histogram reg "kafka_odradek_e2e_message_age_ms"                         "Message age at fetch (pre-commit)"                               label-names-arr)
             :full-e2e                    (register-histogram reg "kafka_odradek_full_e2e_ms"                                "Full produce-to-commit latency ms"                               label-names-arr)
             :topic-config-info           (register-gauge     reg "kafka_odradek_topic_config"
                                                              "Effective topic configuration from Kafka AdminClient"
                                                              topic-config-label-names)
             ;; Topic-info scrape process metrics (per cluster)
             :topic-scrape-duration       (register-topic-scrape-histogram reg "kafka_odradek_topic_scrape_duration_seconds"        "Time for the full topic scrape cycle per cluster (seconds)"      topic-info-cluster-label-names)
             :topic-list-duration         (register-topic-scrape-histogram reg "kafka_odradek_topic_list_duration_seconds"          "Time to list all topics per cluster (seconds)"                   topic-info-cluster-label-names)
             :topic-describe-duration     (register-topic-scrape-histogram reg "kafka_odradek_topic_describe_duration_seconds"      "Time to describe all topics per cluster (seconds)"               topic-info-cluster-label-names)
             :topic-config-describe-duration (register-topic-scrape-histogram reg "kafka_odradek_topic_config_describe_duration_seconds" "Time to describe all topic configs per cluster (seconds)"   topic-info-cluster-label-names)
             :topic-scrape-errors         (register-counter   reg "kafka_odradek_topic_scrape_errors_total"                   "Count of topic scrape errors by step"                            topic-info-cluster-step-label-names)
             ;; Topic-info structural gauges — always registered, not driven by observe-configs
             :topic-partitions            (register-gauge     reg "kafka_odradek_topic_partitions"                           "Number of partitions for the topic"                              topic-info-topic-label-names)
             :topic-replication-factor    (register-gauge     reg "kafka_odradek_topic_replication_factor"                   "Replication factor for the topic (replica count for partition 0)" topic-info-topic-label-names)
             :topic-min-isr               (register-gauge     reg "kafka_odradek_topic_min_isr"                              "ISR count for partition 0 as min-ISR proxy"                      topic-info-topic-label-names)}
          all-metrics (cond-> base-metrics
                        (seq numeric-config-gauges)
                          (assoc :topic-numeric-config-gauges numeric-config-gauges)
                        (some? topic-info-gauge)
                          (assoc :topic-info-gauge topic-info-gauge))]
      (assoc this
             :registry            reg
             :metrics             all-metrics
             :custom-label-keys   union-custom-label-keys
             :numeric-config-keys numeric-config-key-list
             :string-config-keys  string-config-key-list)))
  (stop [this]
    (assoc this
           :registry            nil
           :metrics             nil
           :custom-label-keys   nil
           :numeric-config-keys nil
           :string-config-keys  nil)))

(defn new-metrics-registry []
  (map->MetricsRegistryComponent {}))

(defn- label-values-array
  "Builds a String array of label values in registration order:
   the 5 standard values followed by each custom label value in sorted key order.
   Uses the sanitized custom-labels lookup map so keys match registration names."
  [custom-label-keys label-values-map]
  (let [sanitized-custom-labels (stringify-custom-labels (:custom-labels label-values-map))
        standard-values         [(str (:cluster_name label-values-map))
                                 (str (:observer label-values-map))
                                 (str (:topic label-values-map))
                                 (str (:message_size_kb label-values-map))
                                 (str (:configured_rate_interval label-values-map))]
        custom-values           (map (fn [label-key]
                                       (get sanitized-custom-labels label-key ""))
                                     custom-label-keys)]
    (into-array String (concat standard-values custom-values))))

(defn init-production-error-labels
  "Initializes the production error counter to 0 for the given label set.
   Ensures it appears in scrape output even when no errors have occurred,
   so Grafana shows 0 instead of 'no data'."
  [metrics-registry labels]
  (let [label-arr (label-values-array (:custom-label-keys metrics-registry) labels)]
    (-> (get (:metrics metrics-registry) :production-error-total)
        (.labelValues label-arr)
        (.inc 0.0))))

(defn init-fetch-error-labels
  "Initializes the fetch error counter to 0 for the given label set.
   Ensures it appears in scrape output even when no errors have occurred,
   so Grafana shows 0 instead of 'no data'."
  [metrics-registry labels]
  (let [label-arr (label-values-array (:custom-label-keys metrics-registry) labels)]
    (-> (get (:metrics metrics-registry) :fetch-error-total)
        (.labelValues label-arr)
        (.inc 0.0))))

(defn inc-counter [metrics-registry metric-key label-values-map]
  (-> (get (:metrics metrics-registry) metric-key)
      (.labelValues (label-values-array (:custom-label-keys metrics-registry) label-values-map))
      .inc))

(defn observe-histogram [metrics-registry metric-key label-values-map duration-ms]
  (-> (get (:metrics metrics-registry) metric-key)
      (.labelValues (label-values-array (:custom-label-keys metrics-registry) label-values-map))
      (.observe (double duration-ms))))

(defn set-topic-config! [metrics-registry label-map]
  (-> (get (:metrics metrics-registry) :topic-config-info)
      (.labelValues (into-array String
                     (map str [(:cluster_name label-map)
                               (:topic label-map)
                               (:partitions label-map)
                               (:replication_factor label-map)
                               (:partitions_replicas_broker_ids label-map)
                               (:partitions_isr_broker_ids label-map)
                               (:partitions_leader_broker_ids label-map)
                               (:partitions_replicas_broker_racks label-map)
                               (:min_insync_replicas label-map)
                               (:retention_ms label-map)
                               (:retention_bytes label-map)
                               (:cleanup_policy label-map)
                               (:max_message_bytes label-map)
                               (:compression_type label-map)])))
      (.set 1.0)))

(defn clear-topic-config! [metrics-registry]
  (-> (get (:metrics metrics-registry) :topic-config-info)
      .clear))

;;
;; Topic-info scrape metrics helpers
;; These use their own fixed label schemas independent of the union custom-labels mechanism.
;;

(defn observe-topic-scrape-duration!
  "Records the full scrape cycle duration in seconds for the given cluster."
  [metrics-registry cluster-name duration-seconds]
  (-> (get (:metrics metrics-registry) :topic-scrape-duration)
      (.labelValues (into-array String [cluster-name]))
      (.observe (double duration-seconds))))

(defn observe-topic-list-duration!
  "Records the topic listing step duration in seconds for the given cluster."
  [metrics-registry cluster-name duration-seconds]
  (-> (get (:metrics metrics-registry) :topic-list-duration)
      (.labelValues (into-array String [cluster-name]))
      (.observe (double duration-seconds))))

(defn observe-topic-describe-duration!
  "Records the topic description step duration in seconds for the given cluster."
  [metrics-registry cluster-name duration-seconds]
  (-> (get (:metrics metrics-registry) :topic-describe-duration)
      (.labelValues (into-array String [cluster-name]))
      (.observe (double duration-seconds))))

(defn observe-topic-config-describe-duration!
  "Records the topic config description step duration in seconds for the given cluster."
  [metrics-registry cluster-name duration-seconds]
  (-> (get (:metrics metrics-registry) :topic-config-describe-duration)
      (.labelValues (into-array String [cluster-name]))
      (.observe (double duration-seconds))))

(defn init-topic-scrape-errors!
  "Initializes the topic scrape error counter to 0 for the given cluster and step.
   Ensures it appears in scrape output even when no errors have occurred."
  [metrics-registry cluster-name step]
  (-> (get (:metrics metrics-registry) :topic-scrape-errors)
      (.labelValues (into-array String [cluster-name step]))
      (.inc 0.0)))

(defn inc-topic-scrape-errors!
  "Increments the scrape error counter for the given cluster and step label.
   step must be one of: list, describe, describe-config."
  [metrics-registry cluster-name step]
  (-> (get (:metrics metrics-registry) :topic-scrape-errors)
      (.labelValues (into-array String [cluster-name step]))
      .inc))

(defn set-topic-partitions!
  "Sets the partition count gauge for the given cluster and topic."
  [metrics-registry cluster-name topic-name partition-count]
  (-> (get (:metrics metrics-registry) :topic-partitions)
      (.labelValues (into-array String [cluster-name topic-name]))
      (.set (double partition-count))))

(defn set-topic-replication-factor!
  "Sets the replication factor gauge for the given cluster and topic."
  [metrics-registry cluster-name topic-name replication-factor]
  (-> (get (:metrics metrics-registry) :topic-replication-factor)
      (.labelValues (into-array String [cluster-name topic-name]))
      (.set (double replication-factor))))

(defn set-topic-min-isr!
  "Sets the min-ISR proxy gauge (ISR count for partition 0) for the given cluster and topic."
  [metrics-registry cluster-name topic-name min-isr-count]
  (-> (get (:metrics metrics-registry) :topic-min-isr)
      (.labelValues (into-array String [cluster-name topic-name]))
      (.set (double min-isr-count))))

(defn set-topic-numeric-config!
  "Sets the dynamic numeric config gauge for the given cluster, topic, and config key.
   config-key must be one of the numeric-config-keys that was registered at startup.
   value is a Long."
  [metrics-registry cluster-name topic-name config-key value]
  (when-let [gauge (get-in (:metrics metrics-registry) [:topic-numeric-config-gauges config-key])]
    (-> gauge
        (.labelValues (into-array String [cluster-name topic-name]))
        (.set (double value)))))

(defn set-topic-info!
  "Sets the kafka_odradek_topic_string_config gauge where string config values appear as label values.
   string-config-values is a map of sanitized-key (underscore) to string value,
   in the same order as the string-config-keys registered at startup."
  [metrics-registry cluster-name topic-name string-config-values]
  (when-let [topic-info-gauge (get (:metrics metrics-registry) :topic-info-gauge)]
    (let [string-config-keys      (:string-config-keys metrics-registry)
          sanitized-string-values (mapv (fn [config-key]
                                          (let [sanitized-key (str/replace config-key #"[\.\-]" "_")]
                                            (get string-config-values (keyword sanitized-key) "")))
                                        string-config-keys)
          all-label-values        (into-array String
                                    (concat [cluster-name topic-name]
                                            sanitized-string-values))]
      (-> topic-info-gauge
          (.labelValues all-label-values)
          (.set 1.0)))))

(defn scrape [metrics-registry]
  (let [formats (ExpositionFormats/init)
        baos    (ByteArrayOutputStream.)]
    (.write (.getPrometheusTextFormatWriter formats) baos (.scrape (:registry metrics-registry)))
    (.toString baos "UTF-8")))
