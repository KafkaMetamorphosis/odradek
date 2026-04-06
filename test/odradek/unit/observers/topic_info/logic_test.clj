(ns odradek.unit.observers.topic-info.logic-test
  (:require [clojure.test :refer [deftest is testing]]
            [odradek.observers.topic-info.logic :as topic-info-logic])
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
  {"min.insync.replicas" "2"
   "retention.ms"        "604800000"
   "retention.bytes"     "-1"
   "cleanup.policy"      "delete"
   "max.message.bytes"   "1048576"
   "compression.type"    "producer"})

;; ---------------------------------------------------------------------------
;; config->label-map
;; ---------------------------------------------------------------------------

(deftest config->label-map-returns-correct-keyword-keys-and-values
  (testing "given a Config with all 6 entries, returns a map with underscore keyword keys and string values"
    (let [config    (make-config all-config-entries)
          label-map (topic-info-logic/config->label-map config)]
      (is (= {:min_insync_replicas "2"
              :retention_ms        "604800000"
              :retention_bytes     "-1"
              :cleanup_policy      "delete"
              :max_message_bytes   "1048576"
              :compression_type    "producer"}
             label-map)))))

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
;; build-label-values — full assembly
;; ---------------------------------------------------------------------------

(deftest build-label-values-assembles-all-keys
  (testing "merges cluster_name, topic, description labels, and config labels"
    (let [node-1      (make-node 1 "rack-a")
          partition   (make-partition 0 node-1 [node-1] [node-1])
          description (make-topic-description "my-topic" [partition])
          config      (make-config all-config-entries)
          label-map   (topic-info-logic/build-label-values "prod-cluster" "my-topic" description config)]
      (is (= "prod-cluster" (:cluster_name label-map)))
      (is (= "my-topic" (:topic label-map)))
      (is (= "1" (:partitions label-map)))
      (is (= "1" (:replication_factor label-map)))
      (is (= "2" (:min_insync_replicas label-map)))
      (is (= "delete" (:cleanup_policy label-map)))
      (is (= "producer" (:compression_type label-map))))))

;; ---------------------------------------------------------------------------
;; Label ordering guard: build-label-values keys must match
;; topic-config-label-names order used by set-topic-config!
;; ---------------------------------------------------------------------------

(def ^:private expected-label-name-order
  ["cluster_name" "topic" "partitions" "replication_factor"
   "partitions_replicas_broker_ids" "partitions_isr_broker_ids"
   "partitions_leader_broker_ids" "partitions_replicas_broker_racks"
   "min_insync_replicas" "retention_ms" "retention_bytes"
   "cleanup_policy" "max_message_bytes" "compression_type"])

(deftest label-ordering-matches-registry-label-names
  (testing "values extracted in set-topic-config! order produce the correct positional array"
    (let [node-1      (make-node 5 "us-east-1a")
          node-2      (make-node 6 "us-east-1b")
          partition   (make-partition 0 node-1 [node-1 node-2] [node-1 node-2])
          description (make-topic-description "ordering-topic" [partition])
          config      (make-config all-config-entries)
          label-map   (topic-info-logic/build-label-values "my-cluster" "ordering-topic" description config)
          ;; This is the exact extraction order used in registry/set-topic-config!
          positional-values (mapv str [(:cluster_name label-map)
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
                                       (:compression_type label-map)])]
      ;; Guard 1: every label name has a corresponding key in the map
      (doseq [label-name expected-label-name-order]
        (is (contains? label-map (keyword label-name))
            (str "label-map must contain key :" label-name)))
      ;; Guard 2: no extra keys snuck in (exactly 14 labels)
      (is (= (count expected-label-name-order) (count label-map))
          "label-map must have exactly 14 keys matching topic-config-label-names")
      ;; Guard 3: positional values are non-nil strings
      (doseq [[index value] (map-indexed vector positional-values)]
        (is (string? value)
            (str "position " index " (" (nth expected-label-name-order index) ") must be a string"))
        (is (seq value)
            (str "position " index " (" (nth expected-label-name-order index) ") must be non-empty")))
      ;; Guard 4: spot-check specific positions
      (is (= "my-cluster" (nth positional-values 0)))
      (is (= "ordering-topic" (nth positional-values 1)))
      (is (= "1" (nth positional-values 2)))
      (is (= "2" (nth positional-values 3)))
      (is (= "0:5,6" (nth positional-values 4)))
      (is (= "2" (nth positional-values 8)))
      (is (= "producer" (nth positional-values 13))))))
