(ns odradek.components.metrics-registry
  (:require [com.stuartsierra.component :as component])
  (:import [io.prometheus.metrics.core.metrics Counter Gauge Histogram]
           [io.prometheus.metrics.model.registry PrometheusRegistry]
           [io.prometheus.metrics.expositionformats ExpositionFormats]
           [java.io ByteArrayOutputStream]))

(def ^:private label-names
  (into-array String ["cluster_name" "observer" "topic" "message_size_kb" "configured_rate_interval"]))

(def ^:private topic-config-label-names
  (into-array String ["cluster_name" "topic" "partitions" "replication_factor"
                      "partitions_replicas_broker_ids" "partitions_isr_broker_ids"
                      "partitions_leader_broker_ids" "partitions_replicas_broker_racks"
                      "min_insync_replicas" "retention_ms" "retention_bytes"
                      "cleanup_policy" "max_message_bytes" "compression_type"]))

(def ^:private histogram-buckets
  (double-array [5 10 15 25 30 40 50 80 100 250 300 500 800 1000
                 1500 2000 3000 4000 5000 10000 15000 20000 25000 30000]))

(defn- register-counter [registry metric-name help]
  (-> (Counter/builder)
      (.name metric-name)
      (.help help)
      (.labelNames label-names)
      (.register registry)))

(defn- register-gauge [registry metric-name help label-names-arr]
  (-> (Gauge/builder)
      (.name metric-name)
      (.help help)
      (.labelNames label-names-arr)
      (.register registry)))

(defn- register-histogram [registry metric-name help]
  (-> (Histogram/builder)
      (.name metric-name)
      (.help help)
      (.labelNames label-names)
      (.classicUpperBounds histogram-buckets)
      (.register registry)))

(defrecord MetricsRegistryComponent [registry metrics]
  component/Lifecycle
  (start [this]
    (let [reg (PrometheusRegistry.)
          m   {:produced-total         (register-counter   reg "kafka_odradek_messages_produced_total"          "Total messages produced")
               :production-error-total (register-counter   reg "kafka_odradek_messages_production_error_total" "Total production errors")
               :fetched-total          (register-counter   reg "kafka_odradek_messages_fetched_total"          "Total messages fetched")
               :fetch-error-total      (register-counter   reg "kafka_odradek_messages_fetch_error_total"      "Total fetch errors")
               :production-latency     (register-histogram reg "kafka_odradek_messages_production_latency_ms"  "Production ack latency ms")
               :fetch-latency          (register-histogram reg "kafka_odradek_messages_fetch_latency_ms"       "Message fetch latency ms")
               :e2e-message-age        (register-histogram reg "kafka_odradek_e2e_message_age_ms"              "Message age at fetch (pre-commit)")
               :full-e2e               (register-histogram reg "kafka_odradek_full_e2e_ms"                     "Full produce-to-commit latency ms")
               :topic-config-info      (register-gauge     reg "kafka_odradek_topic_config"
                                                           "Effective topic configuration from Kafka AdminClient"
                                                           topic-config-label-names)}]
      (assoc this :registry reg :metrics m)))
  (stop [this]
    (assoc this :registry nil :metrics nil)))

(defn new-metrics-registry []
  (map->MetricsRegistryComponent {}))

(defn- label-values-array [label-values-map]
  (into-array String
    (map str [(:cluster_name label-values-map)
              (:observer label-values-map)
              (:topic label-values-map)
              (:message_size_kb label-values-map)
              (:configured_rate_interval label-values-map)])))

(defn init-error-labels
  "Initializes error counters to 0 for the given label set.
   Ensures they appear in scrape output even when no errors have occurred,
   so Grafana shows 0 instead of 'no data'."
  [metrics-registry labels]
  (let [label-arr (label-values-array labels)]
    (-> (get (:metrics metrics-registry) :production-error-total)
        (.labelValues label-arr)
        (.inc 0.0))
    (-> (get (:metrics metrics-registry) :fetch-error-total)
        (.labelValues label-arr)
        (.inc 0.0))))

(defn inc-counter [metrics-registry metric-key label-values-map]
  (-> (get (:metrics metrics-registry) metric-key)
      (.labelValues (label-values-array label-values-map))
      .inc))

(defn observe-histogram [metrics-registry metric-key label-values-map duration-ms]
  (-> (get (:metrics metrics-registry) metric-key)
      (.labelValues (label-values-array label-values-map))
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
