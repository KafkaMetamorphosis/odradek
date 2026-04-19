(ns odradek.seed
  (:require [cheshire.core :as json]
            [clojure.java.io :as io]
            [clojure.string :as str]
            [clojure.tools.logging :as log]
            [odradek.clients.kafka :as kafka-client])
  (:import [java.util UUID]
           [org.apache.kafka.clients.admin NewTopic]
           [org.apache.kafka.common.errors TopicExistsException]))

;; Each cluster owns a specific set of observer topics:
;;
;;   local-1 (localhost:9092):
;;     1KB-TOPIC                  4 partitions   (~10 KB/interval)
;;     10KB-TOPIC                 4 partitions   (~100 KB/interval)
;;     100KB-TOPIC                4 partitions   (~100 KB/interval)
;;     + 800 random topics
;;
;;   local-2 (localhost:9094):
;;     1MB-MESSAGES-SMALL-TOPIC  16 partitions  (~3072 KB/interval)
;;
;;   local-3 (localhost:9096):
;;     5MB-MESSAGES-SMALL-TOPIC 128 partitions  (~10240 KB/interval)
;;     9MB-MESSAGES-SMALL-TOPIC  64 partitions  (~9216 KB/interval)
;;
;; Partition counts are derived from relative throughput.
;; Topic names match config.json exactly — no suffixes appended.

(def ^:private topic-partition-assignments
  {"1KB-TOPIC"                4
   "10KB-TOPIC"               4
   "100KB-TOPIC"              4
   "1MB-MESSAGES-SMALL-TOPIC" 16
   "9MB-MESSAGES-SMALL-TOPIC" 64
   "5MB-MESSAGES-SMALL-TOPIC" 128})

(def ^:private replication-factor (short 1))
(def ^:private topic-retention-ms "900000")
(def ^:private topic-retention-bytes "52428800")
(def ^:private random-topic-partition-options [1 2 3])
(def ^:private random-topic-count 800)

(defn- load-config []
  (with-open [config-reader (io/reader (io/resource "config.json"))]
    (json/parse-stream config-reader true)))

(defn- partition-count-for-topic [topic-name]
  (or (get topic-partition-assignments topic-name)
      (throw (ex-info (str "No partition assignment found for topic: " topic-name
                           ". Add it to topic-partition-assignments in dev/odradek/seed.clj.")
                      {:topic-name topic-name}))))

(defn- new-topic-descriptor [topic-name partition-count]
  (doto (NewTopic. topic-name partition-count replication-factor)
    (.configs {"retention.ms"    topic-retention-ms
               "retention.bytes" topic-retention-bytes})))

(defn- create-topic-or-skip! [admin-client topic-name]
  (let [partition-count  (partition-count-for-topic topic-name)
        topic-descriptor (new-topic-descriptor topic-name partition-count)
        creation-results (.createTopics admin-client [topic-descriptor])
        topic-future     (-> creation-results .values (.get topic-name))]
    (try
      (.get topic-future)
      (log/infof "Created topic: %s (%d partitions)" topic-name partition-count)
      (catch java.util.concurrent.ExecutionException execution-exception
        (if (instance? TopicExistsException (.getCause execution-exception))
          (log/infof "Topic already exists, skipping: %s" topic-name)
          (throw execution-exception))))))

(defn- random-partition-count []
  (rand-nth random-topic-partition-options))

(defn- generate-random-topic-names [count]
  (->> (repeatedly count #(str "RANDOM-" (str/upper-case (str (UUID/randomUUID)))))
       vec))

(defn- create-random-topic-or-skip! [admin-client topic-name]
  (let [partition-count  (random-partition-count)
        topic-descriptor (new-topic-descriptor topic-name partition-count)
        creation-results (.createTopics admin-client [topic-descriptor])
        topic-future     (-> creation-results .values (.get topic-name))]
    (try
      (.get topic-future)
      (log/infof "Created random topic: %s (%d partitions)" topic-name partition-count)
      (catch java.util.concurrent.ExecutionException execution-exception
        (if (instance? TopicExistsException (.getCause execution-exception))
          (log/infof "Topic already exists, skipping: %s" topic-name)
          (throw execution-exception))))))

(defn- seed-cluster-1! [bootstrap-url]
  (log/infof "Seeding cluster local-1 at %s..." bootstrap-url)
  (let [cluster-1-topics ["1KB-TOPIC" "10KB-TOPIC" "100KB-TOPIC"]
        random-topic-names (generate-random-topic-names random-topic-count)]
    (with-open [admin-client (kafka-client/new-admin-client bootstrap-url)]
      (doseq [topic-name cluster-1-topics]
        (create-topic-or-skip! admin-client topic-name))
      (doseq [topic-name random-topic-names]
        (create-random-topic-or-skip! admin-client topic-name))))
  (log/infof "Cluster local-1 seed complete (%d observer topics + %d random topics)."
             3 random-topic-count))

(defn- seed-cluster-2! [bootstrap-url]
  (log/infof "Seeding cluster local-2 at %s..." bootstrap-url)
  (with-open [admin-client (kafka-client/new-admin-client bootstrap-url)]
    (create-topic-or-skip! admin-client "1MB-MESSAGES-SMALL-TOPIC"))
  (log/info "Cluster local-2 seed complete."))

(defn- seed-cluster-3! [bootstrap-url]
  (log/infof "Seeding cluster local-3 at %s..." bootstrap-url)
  (with-open [admin-client (kafka-client/new-admin-client bootstrap-url)]
    (create-topic-or-skip! admin-client "5MB-MESSAGES-SMALL-TOPIC")
    (create-topic-or-skip! admin-client "9MB-MESSAGES-SMALL-TOPIC"))
  (log/info "Cluster local-3 seed complete."))

(defn -main [& _args]
  (log/info "Starting Odradek topic seed across all 3 clusters...")
  (let [config        (load-config)
        cluster-1-url (get-in config [:kafka_clusters :local-1 :bootstrap-url])
        cluster-2-url (get-in config [:kafka_clusters :local-2 :bootstrap-url])
        cluster-3-url (get-in config [:kafka_clusters :local-3 :bootstrap-url])]
    (seed-cluster-1! cluster-1-url)
    (seed-cluster-2! cluster-2-url)
    (seed-cluster-3! cluster-3-url))
  (log/info "All cluster seeds complete.")
  (System/exit 0))
