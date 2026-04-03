(ns odradek.components.metrics-registry
  (:require [com.stuartsierra.component :as component])
  (:import [io.prometheus.metrics.core.metrics Counter Histogram]
           [io.prometheus.metrics.model.registry PrometheusRegistry]
           [io.prometheus.metrics.expositionformats ExpositionFormats]
           [java.io ByteArrayOutputStream]))

(def ^:private label-names
  (into-array String ["cluster_name" "observer" "topic" "message_size_kb" "configured_rate_interval"]))

(def ^:private histogram-buckets
  (double-array [5 10 15 25 30 40 50 80 100 250 300 500 800 1000
                 1500 2000 3000 4000 5000 10000 15000 20000 25000 30000]))

(defn- register-counter [registry metric-name help]
  (-> (Counter/builder)
      (.name metric-name)
      (.help help)
      (.labelNames label-names)
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
               :full-e2e               (register-histogram reg "kafka_odradek_full_e2e_ms"                     "Full produce-to-commit latency ms")}]
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

(defn inc-counter [metrics-registry metric-key label-values-map]
  (-> (get (:metrics metrics-registry) metric-key)
      (.labelValues (label-values-array label-values-map))
      .inc))

(defn observe-histogram [metrics-registry metric-key label-values-map duration-ms]
  (-> (get (:metrics metrics-registry) metric-key)
      (.labelValues (label-values-array label-values-map))
      (.observe (double duration-ms))))

(defn scrape [metrics-registry]
  (let [formats (ExpositionFormats/init)
        baos    (ByteArrayOutputStream.)]
    (.write (.getPrometheusTextFormatWriter formats) baos (.metricSnapshots (:registry metrics-registry)))
    (.toString baos "UTF-8")))
