(ns odradek.logic.topic-config)

(defn cluster-topic-pairs
  "Given the observers vector from config, returns a deduplicated sequence
   of [cluster-name topic] pairs."
  [observers]
  (distinct
    (for [observer observers
          cluster-name (:clusters observer)]
      [cluster-name (:topic observer)])))

(defn topic-description->label-map
  "Extracts partitions and replication_factor from a TopicDescription Java object."
  [topic-description]
  {:partitions         (str (.size (.partitions topic-description)))
   :replication_factor (str (.size (.replicas (first (.partitions topic-description)))))})

(defn config->label-map
  "Extracts topic configuration values from a Kafka Config Java object.
   Returns a map with underscore-delimited keys and string values."
  [config]
  (letfn [(config-value [config-key]
            (.value (.get config config-key)))]
    {:min_insync_replicas (config-value "min.insync.replicas")
     :retention_ms        (config-value "retention.ms")
     :retention_bytes     (config-value "retention.bytes")
     :cleanup_policy      (config-value "cleanup.policy")
     :max_message_bytes   (config-value "max.message.bytes")
     :compression_type    (config-value "compression.type")}))

(defn build-label-values
  "Combines cluster name, topic, topic description, and config into the full
   10-label values map for the kafka_odradek_topic_config gauge."
  [cluster-name topic topic-description config]
  (merge {:cluster_name cluster-name
          :topic        topic}
         (topic-description->label-map topic-description)
         (config->label-map config)))
