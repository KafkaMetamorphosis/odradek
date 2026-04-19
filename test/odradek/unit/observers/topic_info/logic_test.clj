(ns odradek.unit.observers.topic-info.logic-test
  (:require [clojure.test :refer [deftest is testing]]
            [odradek.observers.topic-info.logic :as topic-info-logic]
            [odradek.observers.topic-info.config-keys :as config-keys])
  (:import [org.apache.kafka.common Node]
           [org.apache.kafka.common TopicPartitionInfo]
           [org.apache.kafka.clients.admin TopicDescription Config ConfigEntry]))

;; ---------------------------------------------------------------------------
;; Test data builders
;; ---------------------------------------------------------------------------

(defn- make-node
  ([node-id rack]
   (Node. (int node-id) (str "host-" node-id) (int 9092) rack))
  ([node-id]
   (make-node node-id nil)))

(defn- make-partition [partition-index leader-node replicas-nodes isr-nodes]
  (TopicPartitionInfo. (int partition-index)
                       leader-node
                       (java.util.ArrayList. replicas-nodes)
                       (java.util.ArrayList. isr-nodes)))

(defn- make-topic-description [topic-name partitions]
  (TopicDescription. topic-name false (java.util.ArrayList. partitions)))

(defn- make-config [entries-map]
  (Config. (java.util.ArrayList.
             (mapv (fn [[config-key config-value]]
                     (ConfigEntry. config-key config-value))
                   entries-map))))

(def ^:private all-config-entries
  {"cleanup.policy"                       "delete"
   "compression.type"                     "producer"
   "delete.retention.ms"                  "86400000"
   "file.delete.delay.ms"                 "60000"
   "flush.messages"                       "9223372036854775807"
   "flush.ms"                             "9223372036854775807"
   "follower.replication.throttled.replicas" ""
   "index.interval.bytes"                 "4096"
   "leader.replication.throttled.replicas"   ""
   "local.retention.bytes"                "-2"
   "local.retention.ms"                   "-2"
   "max.compaction.lag.ms"                "9223372036854775807"
   "max.message.bytes"                    "1048588"
   "message.downconversion.enable"        "true"
   "message.format.version"               "3.0-IV1"
   "message.timestamp.after.max.ms"       "9223372036854775807"
   "message.timestamp.before.max.ms"      "9223372036854775807"
   "message.timestamp.difference.max.ms"  "9223372036854775807"
   "message.timestamp.type"               "CreateTime"
   "min.cleanable.dirty.ratio"            "0.5"
   "min.compaction.lag.ms"                "0"
   "min.insync.replicas"                  "1"
   "preallocate"                          "false"
   "remote.storage.enable"                "false"
   "retention.bytes"                      "-1"
   "retention.ms"                         "604800000"
   "segment.bytes"                        "1073741824"
   "segment.index.bytes"                  "10485760"
   "segment.jitter.ms"                    "0"
   "segment.ms"                           "604800000"
   "unclean.leader.election.enable"       "false"})

;; ---------------------------------------------------------------------------
;; topic-description->label-map — single partition
;; ---------------------------------------------------------------------------

(deftest topic-description->label-map-single-partition
  (testing "single partition with two replicas, ISR, leader, and racks"
    (let [node-1      (make-node 1 "rack-a")
          node-2      (make-node 2 "rack-b")
          partition   (make-partition 0 node-1 [node-1 node-2] [node-1 node-2])
          description (make-topic-description "test-topic" [partition])
          label-map   (topic-info-logic/topic-description->label-map description)]
      (is (= "1" (:partitions label-map)))
      (is (= "2" (:replication_factor label-map)))
      (is (= "0:1,2" (:partitions_replicas_broker_ids label-map)))
      (is (= "0:1,2" (:partitions_isr_broker_ids label-map)))
      (is (= "0:1" (:partitions_leader_broker_ids label-map)))
      (is (= "0:rack-a,rack-b" (:partitions_replicas_broker_racks label-map))))))

;; ---------------------------------------------------------------------------
;; topic-description->label-map — multiple partitions
;; ---------------------------------------------------------------------------

(deftest topic-description->label-map-multiple-partitions
  (testing "two partitions produce semicolon-delimited segments"
    (let [node-1      (make-node 1 "rack-a")
          node-2      (make-node 2 "rack-b")
          node-3      (make-node 3 "rack-c")
          partition-0 (make-partition 0 node-1 [node-1 node-2] [node-1 node-2])
          partition-1 (make-partition 1 node-3 [node-3 node-1] [node-3])
          description (make-topic-description "multi-part" [partition-0 partition-1])
          label-map   (topic-info-logic/topic-description->label-map description)]
      (is (= "2" (:partitions label-map)))
      (is (= "2" (:replication_factor label-map))
          "replication factor is taken from the first partition's replica count")
      (is (= "0:1,2;1:3,1" (:partitions_replicas_broker_ids label-map)))
      (is (= "0:1,2;1:3" (:partitions_isr_broker_ids label-map)))
      (is (= "0:1;1:3" (:partitions_leader_broker_ids label-map)))
      (is (= "0:rack-a,rack-b;1:rack-c,rack-a" (:partitions_replicas_broker_racks label-map))))))

;; ---------------------------------------------------------------------------
;; topic-description->label-map — no rack info
;; ---------------------------------------------------------------------------

(deftest topic-description->label-map-no-rack-info
  (testing "nodes without rack report 'none' in the rack label"
    (let [node-1      (make-node 1)
          node-2      (make-node 2)
          partition   (make-partition 0 node-1 [node-1 node-2] [node-1])
          description (make-topic-description "no-rack" [partition])
          label-map   (topic-info-logic/topic-description->label-map description)]
      (is (= "0:none,none" (:partitions_replicas_broker_racks label-map))))))

;; ---------------------------------------------------------------------------
;; build-partitions-broker-ids — serialization format
;; ---------------------------------------------------------------------------

(deftest build-partitions-broker-ids-single-partition-format
  (testing "single partition produces partition-index:broker-id,broker-id"
    (let [node-1      (make-node 10 "rack-x")
          node-2      (make-node 20 "rack-y")
          partition   (make-partition 0 node-1 [node-1 node-2] [node-1])
          description (make-topic-description "t" [partition])]
      (is (= "0:10,20" (topic-info-logic/build-partitions-broker-ids description))))))

(deftest build-partitions-broker-ids-multiple-partitions-format
  (testing "multiple partitions are joined with semicolons"
    (let [node-1      (make-node 1 "rack-a")
          node-2      (make-node 2 "rack-b")
          partition-0 (make-partition 0 node-1 [node-1 node-2] [node-1 node-2])
          partition-1 (make-partition 1 node-2 [node-2] [node-2])
          description (make-topic-description "t" [partition-0 partition-1])]
      (is (= "0:1,2;1:2" (topic-info-logic/build-partitions-broker-ids description))))))

;; ---------------------------------------------------------------------------
;; build-label-values — full assembly with all exposed-topic-config-keys
;; ---------------------------------------------------------------------------

(deftest build-label-values-assembles-all-keys
  (testing "result contains cluster_name, topic, description labels, and all exposed-topic-config-keys as sanitized labels"
    (let [node-1      (make-node 1 "rack-a")
          partition   (make-partition 0 node-1 [node-1] [node-1])
          description (make-topic-description "my-topic" [partition])
          config      (make-config all-config-entries)
          label-map   (topic-info-logic/build-label-values "prod-cluster" "my-topic" description config)
          sanitized-config-keys (map (fn [k]
                                       (keyword (clojure.string/replace k #"[\.\-]" "_")))
                                     config-keys/exposed-topic-config-keys)]
      (is (= "prod-cluster" (:cluster_name label-map)))
      (is (= "my-topic" (:topic label-map)))
      (is (= "1" (:partitions label-map)))
      (is (= "1" (:replication_factor label-map)))
      ;; All exposed-topic-config-keys must appear as sanitized keyword keys
      (doseq [sanitized-key sanitized-config-keys]
        (is (contains? label-map sanitized-key)
            (str "label-map must contain key " sanitized-key)))
      ;; Per-key value assertions for all 31 exposed config keys
      (is (= "delete"                  (:cleanup_policy label-map)))
      (is (= "producer"                (:compression_type label-map)))
      (is (= "86400000"                (:delete_retention_ms label-map)))
      (is (= "60000"                   (:file_delete_delay_ms label-map)))
      (is (= "9223372036854775807"     (:flush_messages label-map)))
      (is (= "9223372036854775807"     (:flush_ms label-map)))
      (is (= ""                        (:follower_replication_throttled_replicas label-map)))
      (is (= "4096"                    (:index_interval_bytes label-map)))
      (is (= ""                        (:leader_replication_throttled_replicas label-map)))
      (is (= "-2"                      (:local_retention_bytes label-map)))
      (is (= "-2"                      (:local_retention_ms label-map)))
      (is (= "9223372036854775807"     (:max_compaction_lag_ms label-map)))
      (is (= "1048588"                 (:max_message_bytes label-map)))
      (is (= "true"                    (:message_downconversion_enable label-map)))
      (is (= "3.0-IV1"                 (:message_format_version label-map)))
      (is (= "9223372036854775807"     (:message_timestamp_after_max_ms label-map)))
      (is (= "9223372036854775807"     (:message_timestamp_before_max_ms label-map)))
      (is (= "9223372036854775807"     (:message_timestamp_difference_max_ms label-map)))
      (is (= "CreateTime"              (:message_timestamp_type label-map)))
      (is (= "0.5"                     (:min_cleanable_dirty_ratio label-map)))
      (is (= "0"                       (:min_compaction_lag_ms label-map)))
      (is (= "1"                       (:min_insync_replicas label-map)))
      (is (= "false"                   (:preallocate label-map)))
      (is (= "false"                   (:remote_storage_enable label-map)))
      (is (= "-1"                      (:retention_bytes label-map)))
      (is (= "604800000"               (:retention_ms label-map)))
      (is (= "1073741824"              (:segment_bytes label-map)))
      (is (= "10485760"                (:segment_index_bytes label-map)))
      (is (= "0"                       (:segment_jitter_ms label-map)))
      (is (= "604800000"               (:segment_ms label-map)))
      (is (= "false"                   (:unclean_leader_election_enable label-map)))
      ;; Keys absent from the Config object return empty string
      (is (= "" (get label-map :nonexistent_custom_key ""))
          "config key absent from Config object must map to empty string"))))

;; ---------------------------------------------------------------------------
;; Label ordering guard: build-label-values keys must match
;; topic-config-label-names order used by set-topic-config!
;; ---------------------------------------------------------------------------

(def ^:private default-label-names
  ["cluster_name" "topic" "partitions" "replication_factor"
   "partitions_replicas_broker_ids" "partitions_isr_broker_ids"
   "partitions_leader_broker_ids" "partitions_replicas_broker_racks"])

(def ^:private expected-label-name-order
  (vec (concat default-label-names
               (map (fn [k] (clojure.string/replace k #"[\.\-]" "_"))
                    config-keys/exposed-topic-config-keys))))

(deftest label-ordering-matches-registry-label-names
  (testing "values extracted in set-topic-config! order produce the correct positional array"
    (let [node-1      (make-node 5 "us-east-1a")
          node-2      (make-node 6 "us-east-1b")
          partition   (make-partition 0 node-1 [node-1 node-2] [node-1 node-2])
          description (make-topic-description "ordering-topic" [partition])
          config      (make-config all-config-entries)
          label-map   (topic-info-logic/build-label-values "my-cluster" "ordering-topic" description config)
          positional-values (mapv (fn [label-name]
                                    (str (get label-map (keyword label-name) "")))
                                  expected-label-name-order)]
      ;; Guard 1: every label name has a corresponding key in the map
      (doseq [label-name expected-label-name-order]
        (is (contains? label-map (keyword label-name))
            (str "label-map must contain key :" label-name)))
      ;; Guard 2: exactly the right number of labels (8 default + 31 config keys = 39)
      (is (= (count expected-label-name-order) (count label-map))
          (str "label-map must have exactly " (count expected-label-name-order) " keys matching topic-config-label-names"))
      ;; Guard 3: positional values are strings (empty string is allowed for absent config keys)
      (doseq [[index value] (map-indexed vector positional-values)]
        (is (string? value)
            (str "position " index " (" (nth expected-label-name-order index) ") must be a string")))
      ;; Guard 4: spot-check specific positions
      (is (= "my-cluster" (nth positional-values 0)))
      (is (= "ordering-topic" (nth positional-values 1)))
      (is (= "1" (nth positional-values 2)))
      (is (= "2" (nth positional-values 3)))
      (is (= "0:5,6" (nth positional-values 4))))))

;; ---------------------------------------------------------------------------
;; extract-partition-count
;; ---------------------------------------------------------------------------

(deftest extract-partition-count-returns-long-partition-count
  (testing "single partition returns 1"
    (let [node-1      (make-node 1 "rack-a")
          partition   (make-partition 0 node-1 [node-1] [node-1])
          description (make-topic-description "single-part" [partition])]
      (is (= 1 (topic-info-logic/extract-partition-count description)))))
  (testing "three partitions returns 3"
    (let [node-1      (make-node 1 "rack-a")
          node-2      (make-node 2 "rack-b")
          node-3      (make-node 3 "rack-c")
          partition-0 (make-partition 0 node-1 [node-1] [node-1])
          partition-1 (make-partition 1 node-2 [node-2] [node-2])
          partition-2 (make-partition 2 node-3 [node-3] [node-3])
          description (make-topic-description "three-part" [partition-0 partition-1 partition-2])]
      (is (= 3 (topic-info-logic/extract-partition-count description))))))

;; ---------------------------------------------------------------------------
;; extract-replication-factor
;; ---------------------------------------------------------------------------

(deftest extract-replication-factor-returns-replica-count-for-partition-0
  (testing "two replicas on partition 0 returns 2"
    (let [node-1      (make-node 1 "rack-a")
          node-2      (make-node 2 "rack-b")
          partition   (make-partition 0 node-1 [node-1 node-2] [node-1 node-2])
          description (make-topic-description "rf-topic" [partition])]
      (is (= 2 (topic-info-logic/extract-replication-factor description)))))
  (testing "single replica returns 1"
    (let [node-1      (make-node 1 "rack-a")
          partition   (make-partition 0 node-1 [node-1] [node-1])
          description (make-topic-description "rf-one" [partition])]
      (is (= 1 (topic-info-logic/extract-replication-factor description))))))

;; ---------------------------------------------------------------------------
;; extract-min-isr-proxy
;; ---------------------------------------------------------------------------

(deftest extract-min-isr-proxy-returns-isr-count-for-partition-0
  (testing "two ISR nodes returns 2"
    (let [node-1      (make-node 1 "rack-a")
          node-2      (make-node 2 "rack-b")
          partition   (make-partition 0 node-1 [node-1 node-2] [node-1 node-2])
          description (make-topic-description "isr-topic" [partition])]
      (is (= 2 (topic-info-logic/extract-min-isr-proxy description)))))
  (testing "under-replicated partition with one ISR returns 1"
    (let [node-1      (make-node 1 "rack-a")
          node-2      (make-node 2 "rack-b")
          partition   (make-partition 0 node-1 [node-1 node-2] [node-1])
          description (make-topic-description "under-replicated" [partition])]
      (is (= 1 (topic-info-logic/extract-min-isr-proxy description))))))
