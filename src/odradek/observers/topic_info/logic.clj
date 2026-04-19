(ns odradek.observers.topic-info.logic
  (:require [clojure.string :as string]))


(def exposed-topic-config-keys
  ["cleanup.policy" "compression.type" "delete.retention.ms" "file.delete.delay.ms"
   "flush.messages" "flush.ms" "follower.replication.throttled.replicas"
   "index.interval.bytes" "leader.replication.throttled.replicas"
   "local.retention.bytes" "local.retention.ms" "max.compaction.lag.ms"
   "max.message.bytes" "message.downconversion.enable" "message.format.version"
   "message.timestamp.after.max.ms" "message.timestamp.before.max.ms"
   "message.timestamp.difference.max.ms" "message.timestamp.type"
   "min.cleanable.dirty.ratio" "min.compaction.lag.ms" "min.insync.replicas"
   "preallocate" "remote.storage.enable" "retention.bytes" "retention.ms"
   "segment.bytes" "segment.index.bytes" "segment.jitter.ms" "segment.ms"
   "unclean.leader.election.enable"])


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

(defn- topic-configs->label-map
  "Extracts all keys in exposed-topic-config-keys from a Kafka Config object as labels.
   Returns a map of sanitized keyword (underscore) to string value.
   Keys absent from the Config object return empty string."
  [config]
  (into {}
    (map (fn [config-key]
           (let [config-entry (.get config config-key)
                 label-key    (keyword (string/replace config-key #"[\.\-]" "_"))
                 label-value  (if (some? config-entry) (.value config-entry) "")]
             [label-key label-value]))
         exposed-topic-config-keys)))

(defn build-label-values
  "Combines cluster name, topic, topic description, and config into the full
   label values map for the kafka_odradek_topic_config info gauge.
   Returns a flat map with keyword keys matching all registered label names."
  [cluster-name topic topic-description config]
  (merge {:cluster_name cluster-name
          :topic        topic}
         (topic-description->label-map topic-description)
         (topic-configs->label-map config)))

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
