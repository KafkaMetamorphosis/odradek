(ns odradek.metrics.registry
  (:require [com.stuartsierra.component :as component]
            [clojure.string :as str])
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

(def ^:private histogram-buckets
  (double-array [5 10 15 25 30 40 50 80 100 250 300 500 800 1000
                 1500 2000 3000 4000 5000 10000 15000 20000 25000 30000]))

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

(defrecord MetricsRegistryComponent [config registry metrics custom-label-keys]
  component/Lifecycle
  (start [this]
    (let [observers              (get-in this [:config :config :observers])
          union-custom-label-keys (compute-union-custom-label-keys observers)
          label-names-arr        (build-label-names-array union-custom-label-keys)
          reg                    (PrometheusRegistry.)
          m   {:produced-total         (register-counter   reg "kafka_odradek_messages_produced_total"          "Total messages produced"              label-names-arr)
               :production-error-total (register-counter   reg "kafka_odradek_messages_production_error_total" "Total production errors"              label-names-arr)
               :fetched-total          (register-counter   reg "kafka_odradek_messages_fetched_total"          "Total messages fetched"               label-names-arr)
               :fetch-error-total      (register-counter   reg "kafka_odradek_messages_fetch_error_total"      "Total fetch errors"                   label-names-arr)
               :production-latency     (register-histogram reg "kafka_odradek_messages_production_latency_ms"  "Production ack latency ms"            label-names-arr)
               :fetch-latency          (register-histogram reg "kafka_odradek_messages_fetch_latency_ms"       "Message fetch latency ms"             label-names-arr)
               :e2e-message-age        (register-histogram reg "kafka_odradek_e2e_message_age_ms"              "Message age at fetch (pre-commit)"    label-names-arr)
               :full-e2e               (register-histogram reg "kafka_odradek_full_e2e_ms"                     "Full produce-to-commit latency ms"    label-names-arr)
               :topic-config-info      (register-gauge     reg "kafka_odradek_topic_config"
                                                           "Effective topic configuration from Kafka AdminClient"
                                                           topic-config-label-names)}]
      (assoc this
             :registry          reg
             :metrics           m
             :custom-label-keys union-custom-label-keys)))
  (stop [this]
    (assoc this :registry nil :metrics nil :custom-label-keys nil)))

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

(defn scrape [metrics-registry]
  (let [formats (ExpositionFormats/init)
        baos    (ByteArrayOutputStream.)]
    (.write (.getPrometheusTextFormatWriter formats) baos (.scrape (:registry metrics-registry)))
    (.toString baos "UTF-8")))
