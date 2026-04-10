(ns odradek.seed
  (:require [cheshire.core :as json]
            [clojure.java.io :as io]
            [clojure.string :as str]
            [clojure.tools.logging :as log]
            [odradek.clients.kafka :as kafka-client])
  (:import [java.util UUID]
           [org.apache.kafka.clients.admin NewTopic]
           [org.apache.kafka.common.errors TopicExistsException]))

;; Partition counts are assigned based on relative throughput derived from each
;; topic's observer volume-config (message-size-kb * messages-per-interval):
;;
;;   SMALL-TOPIC                4 partitions   (~100 KB/interval)
;;   1MB-MESSAGES-SMALL-TOPIC  16 partitions  (~3072 KB/interval)
;;   9MB-MESSAGES-SMALL-TOPIC  64 partitions  (~9216 KB/interval)
;;   5MB-MESSAGES-SMALL-TOPIC 128 partitions  (~10240 KB/interval)
;;
;; Each topic is created exactly once with its assigned partition count.
;; No suffixes are appended — topic names match config.json exactly.
(def ^:private topic-partition-assignments
  {"SMALL-TOPIC"              4
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

(defn- extract-unique-topic-names [observers]
  (->> observers
       (keep :topic)
       distinct
       vec))

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

(defn -main [& _args]
  (log/info "Starting Odradek topic seed...")
  (let [config             (load-config)
        bootstrap-url      (get-in config [:kafka_clusters :local-1 :bootstrap-url])
        unique-topic-names (extract-unique-topic-names (:observers config))
        random-topic-names (generate-random-topic-names random-topic-count)]
    (log/infof "Bootstrap URL: %s" bootstrap-url)
    (log/infof "Seeding %d observer topics and %d random topics..."
               (count unique-topic-names)
               (count random-topic-names))
    (with-open [admin-client (kafka-client/new-admin-client bootstrap-url)]
      (doseq [topic-name unique-topic-names]
        (create-topic-or-skip! admin-client topic-name))
      (doseq [topic-name random-topic-names]
        (create-random-topic-or-skip! admin-client topic-name))))
  (log/info "Seed complete.")
  (System/exit 0))
