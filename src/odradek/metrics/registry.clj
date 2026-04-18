(ns odradek.metrics.registry
  (:require [com.stuartsierra.component :as component]
            [clojure.string :as str]
            [clojure.tools.logging :as log])
  (:import [io.prometheus.metrics.core.metrics Counter Gauge Histogram]
           [io.prometheus.metrics.model.registry PrometheusRegistry]
           [io.prometheus.metrics.expositionformats ExpositionFormats]
           [java.io ByteArrayOutputStream]))

(def ^:private base-labels
  ["cluster_name" "observer" "topic" "message_size_kb" "configured_rate_interval"])

(def ^:private default-topic-config-label-names
  ["cluster_name"
   "topic"
   "partitions"
   "replication_factor"
   "partitions_replicas_broker_ids"
   "partitions_isr_broker_ids"
   "partitions_leader_broker_ids"
   "partitions_replicas_broker_racks"])

(def ^:private topic-info-cluster-label-names
  (into-array String ["cluster_name"]))

(def ^:private topic-info-cluster-step-label-names
  (into-array String ["cluster_name" "step"]))

(def ^:private topic-scrape-histogram-buckets
  (double-array [0.001 0.005 0.01 0.025 0.05 0.1 0.25 0.5 1.0 2.5 5.0 10.0 30.0 60.0]))

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
   Does not include base-labels. Used by label-values-array to extract
   custom label values in registration order."
  [observers]
  (->> observers
       (mapcat (fn [observer]
                 (keys (:custom-labels observer))))
       (map sanitize-label-name)
       set
       sort
       vec))

(defn- build-merged-label-names-array
  "Builds the full String array of label names for producer/consumer metrics:
   base-labels followed by all union custom label key strings."
  [union-custom-label-keys]
  (into-array String (concat base-labels union-custom-label-keys)))

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

(defn- register-histogram [registry metric-name help label-names-arr buckets]
  (-> (Histogram/builder)
      (.name metric-name)
      (.help help)
      (.labelNames label-names-arr)
      (.classicUpperBounds buckets)
      (.register registry)))

(defn- find-topic-info-observer
  "Returns the first observer map with observer-type 'topic-info', or nil."
  [observers]
  (first (filter #(= "topic-info" (:observer-type %)) observers)))

(defn- topic-info-labels [topic-info-observer]
  (let [default-label-set    (set default-topic-config-label-names)
        observe-config-names (->> (:observe-configs topic-info-observer)
                                  (map sanitize-label-name)
                                  (remove default-label-set))]
    (into-array String (concat default-topic-config-label-names observe-config-names))))

(defn- topic-info-ordered-label-names
  "Returns an ordered vector of all label name strings for the topic-config gauge,
   matching the registration order used in topic-info-labels.
   Used by set-topic-config! to build the positional values array."
  [topic-info-observer]
  (let [default-label-set    (set default-topic-config-label-names)
        observe-config-names (->> (:observe-configs topic-info-observer)
                                  (map sanitize-label-name)
                                  (remove default-label-set))]
    (vec (concat default-topic-config-label-names observe-config-names))))


(defrecord MetricsRegistryComponent [config registry metrics custom-label-keys]
  component/Lifecycle
  (start [this]
    (log/info "Starting MetricsRegistryComponent ...")
    (let [observers                (get-in config [:config :observers])
          union-custom-label-keys  (compute-union-custom-label-keys observers)
          merged-labels            (build-merged-label-names-array union-custom-label-keys)
          topic-info-observer      (find-topic-info-observer observers)
          reg                      (PrometheusRegistry.) 
          metrics                  {;;producer observer
                                    :produced-total              (register-counter   reg "kafka_odradek_messages_produced_total"                    "Total messages produced"                                         merged-labels)
                                    :production-error-total      (register-counter   reg "kafka_odradek_messages_production_error_total"            "Total production errors"                                         merged-labels)
                                    :production-latency          (register-histogram reg "kafka_odradek_messages_production_latency_ms"             "Production ack latency ms"                                       merged-labels histogram-buckets)
                                   
                                   
                                    ;;consumer observer
                                    :fetched-total               (register-counter   reg "kafka_odradek_messages_fetched_total"                     "Total messages fetched"                                          merged-labels)
                                    :fetch-error-total           (register-counter   reg "kafka_odradek_messages_fetch_error_total"                 "Total fetch errors"                                              merged-labels)
                                    :fetch-latency               (register-histogram reg "kafka_odradek_messages_fetch_latency_ms"                  "Message fetch latency ms"                                        merged-labels histogram-buckets)
                                    :e2e-message-age             (register-histogram reg "kafka_odradek_e2e_message_age_ms"                         "Message age at fetch (pre-commit)"                               merged-labels histogram-buckets)
                                    :full-e2e                    (register-histogram reg "kafka_odradek_full_e2e_ms"                                "Full produce-to-commit latency ms"                               merged-labels histogram-buckets)
                                   
                                    ;; topic info metric
                                    :topic-config                   (register-gauge     reg "kafka_odradek_topic_config"                            "Effective topic configuration from Kafka AdminClient"            (topic-info-labels topic-info-observer))
                                   
                                    ;; Topic-info scrape process metrics (per cluster)
                                    :topic-scrape-duration          (register-histogram reg "kafka_odradek_topic_scrape_duration_seconds"           "Time for the full topic scrape cycle per cluster (seconds)"      topic-info-cluster-label-names       topic-scrape-histogram-buckets)
                                    :topic-list-duration            (register-histogram reg "kafka_odradek_topic_list_duration_seconds"             "Time to list all topics per cluster (seconds)"                   topic-info-cluster-label-names       topic-scrape-histogram-buckets)
                                    :topic-describe-duration        (register-histogram reg "kafka_odradek_topic_describe_duration_seconds"         "Time to describe all topics per cluster (seconds)"               topic-info-cluster-label-names       topic-scrape-histogram-buckets)
                                    :topic-config-describe-duration (register-histogram reg "kafka_odradek_topic_config_describe_duration_seconds"  "Time to describe all topic configs per cluster (seconds)"        topic-info-cluster-label-names       topic-scrape-histogram-buckets)
                                    :topic-scrape-errors            (register-counter   reg "kafka_odradek_topic_scrape_errors_total"               "Count of topic scrape errors by step"                            topic-info-cluster-step-label-names)}]
      (assoc this
             :registry                    reg
             :metrics                     metrics
             :merged-labels               merged-labels
             :custom-label-keys           union-custom-label-keys
             :topic-config-label-names    (topic-info-ordered-label-names topic-info-observer))))
  (stop [this]
    (assoc this
           :registry                 nil
           :metrics                  nil
           :custom-label-keys        nil
           :topic-config-label-names nil)))

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

(defn set-topic-config!
  "Sets the kafka_odradek_topic_config gauge to 1.0 for the given label-map.
   Label values are extracted in the same order as the registered label names:
   default-topic-config-label-names first, then sanitized observe-configs keys.
   label-map must have keyword keys matching each label name."
  [metrics-registry label-map]
  (let [ordered-label-names (:topic-config-label-names metrics-registry)
        label-values-arr    (into-array String
                              (map (fn [label-name]
                                     (str (get label-map (keyword label-name) "")))
                                   ordered-label-names))]
    (-> (get (:metrics metrics-registry) :topic-config)
        (.labelValues label-values-arr)
        (.set 1.0))))

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


(defn scrape [metrics-registry]
  (let [formats (ExpositionFormats/init)
        baos    (ByteArrayOutputStream.)]
    (.write (.getPrometheusTextFormatWriter formats) baos (.scrape (:registry metrics-registry)))
    (.toString baos "UTF-8")))
