(ns odradek.observers.topic-info.logic
  (:require [clojure.string :as string]))


(def numeric-config-keys
  "Set of Kafka topic config keys whose values are numeric and exposed as individual gauges."
  #{"retention.ms" "retention.bytes" "min.insync.replicas" "max.message.bytes"})

(defn build-partitions-broker-ids [topic-description]
  (string/join ";" (reduce (fn [acc partition]
                          (conj acc (str (.partition partition) ":"
                                         (string/join "," (map #(.id %) (.replicas partition))))))
                        []
                        (.partitions topic-description))))

(defn- build-partitions-irs-broker-ids [topic-description]
  (string/join ";" (reduce (fn [acc partition]
                             (conj acc (str (.partition partition) ":"
                                            (string/join "," (map #(.id %) (.isr partition))))))
                           []
                           (.partitions topic-description))))

(defn- build-partitions-leader-broker-id [topic-description]
  (string/join ";" (reduce (fn [acc partition]
                             (conj acc (str (.partition partition) ":" (or (-> partition .leader .id) "none"))))
                           []
                           (.partitions topic-description))))

(defn- build-partitions-replicas-racks [topic-description]
  (string/join ";" (reduce (fn [acc partition]
                             (conj acc (str (.partition partition) ":"
                                            (string/join "," (map (fn [r] (or (.rack r) "none")) (.replicas partition))))))
                           []
                           (.partitions topic-description))))

(defn topic-description->label-map
  "Extracts partitions and replication_factor from a TopicDescription Java object."
  [topic-description]
  {:partitions         (str (.size (.partitions topic-description)))
   :replication_factor (str (.size (.replicas (first (.partitions topic-description)))))
   :partitions_replicas_broker_ids (build-partitions-broker-ids topic-description)
   :partitions_isr_broker_ids (build-partitions-irs-broker-ids topic-description)
   :partitions_leader_broker_ids (build-partitions-leader-broker-id topic-description)
   :partitions_replicas_broker_racks (build-partitions-replicas-racks topic-description)})

(defn- legacy-config->label-map
  "Extracts the fixed set of 6 topic configuration values used by the legacy
   kafka_odradek_topic_config info gauge. Returns a map with underscore-delimited
   keyword keys and string values."
  [config]
  (letfn [(config-value [config-key]
            (.value (.get config config-key)))]
    {:min_insync_replicas (config-value "min.insync.replicas")
     :retention_ms        (config-value "retention.ms")
     :retention_bytes     (config-value "retention.bytes")
     :cleanup_policy      (config-value "cleanup.policy")
     :max_message_bytes   (config-value "max.message.bytes")
     :compression_type    (config-value "compression.type")}))

(defn classify-observe-configs
  "Splits observe-configs into numeric and string categories.
   Numeric keys are those present in numeric-config-keys; all others are strings.
   Returns {:numeric [...] :string [...]} preserving original config key strings."
  [observe-configs]
  (reduce (fn [classification config-key]
            (if (contains? numeric-config-keys config-key)
              (update classification :numeric conj config-key)
              (update classification :string conj config-key)))
          {:numeric [] :string []}
          observe-configs))

(defn config->label-map
  "Extracts string-classified observe-configs from a Kafka Config object as labels.
   Only iterates over the string-classified keys in observe-configs.
   Returns a map of sanitized-key (underscore) to string value.
   If a key is absent from the Config, uses empty string."
  [config string-observe-configs]
  (into {}
    (map (fn [config-key]
           (let [config-entry (.get config config-key)
                 label-key    (keyword (string/replace config-key #"[\.\-]" "_"))
                 label-value  (if (some? config-entry) (.value config-entry) "")]
             [label-key label-value]))
         string-observe-configs)))

(defn build-label-values
  "Combines cluster name, topic, topic description, and config into the full
   label values map for the legacy kafka_odradek_topic_config gauge."
  [cluster-name topic topic-description config]
  (merge {:cluster_name cluster-name
          :topic        topic}
         (topic-description->label-map topic-description)
         (legacy-config->label-map config)))

;;
;; Numeric gauge extraction — used by the per-metric numeric gauges
;;

(defn extract-partition-count
  "Returns the number of partitions for the topic as a long."
  [topic-description]
  (.size (.partitions topic-description)))

(defn extract-replication-factor
  "Returns the replica count for partition 0 as a long.
   This is the conventional replication factor for a uniformly replicated topic."
  [topic-description]
  (.size (.replicas (first (.partitions topic-description)))))

(defn extract-min-isr-proxy
  "Returns the ISR count for partition 0 as a long.
   Used as a per-scrape proxy for the actual min.insync.replicas setting would require.
   A value below replication-factor indicates an under-replicated partition."
  [topic-description]
  (.size (.isr (first (.partitions topic-description)))))

(defn extract-numeric-config-value
  "Parses the numeric value of a Kafka config key from a Config object.
   Returns the value as a Long. Returns -1 if the key is absent or the value
   cannot be parsed as a Long."
  [config config-key]
  (let [config-entry (.get config config-key)]
    (if (some? config-entry)
      (try
        (Long/parseLong (.value config-entry))
        (catch NumberFormatException _
          -1))
      -1)))
