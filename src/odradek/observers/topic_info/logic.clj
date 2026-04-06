(ns odradek.observers.topic-info.logic
  (:require [clojure.string :as string]))


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
