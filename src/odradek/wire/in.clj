(ns odradek.wire.in
  (:require [schema.core :as s]))

(def ProducerConfig {s/Str s/Any})
(def ConsumerConfig {s/Str s/Any})

(def Observer
  {:name                s/Str
   :clusters            [s/Str]
   :topic               s/Str
   :parallelism         s/Int
   :messages-per-bucket s/Int
   :message-size-kb     s/Int
   :producer-config     ProducerConfig
   :consumer-config     ConsumerConfig})

(def KafkaCluster
  {:bootstrap-url s/Str})

(def ProducerEngine
  {:rate-interval-ms s/Int})

(def TopicScraper
  {:scrape-interval-ms s/Int})

(def Config
  {:observers       [Observer]
   :kafka_clusters  {s/Str KafkaCluster}
   :producer-engine ProducerEngine
   :topic-scraper   TopicScraper})

(defn- validate-cluster-references [config]
  (let [known-clusters (set (keys (:kafka_clusters config)))]
    (doseq [observer (:observers config)
            cluster  (:clusters observer)]
      (when-not (contains? known-clusters cluster)
        (throw (ex-info (str "Observer '" (:name observer)
                             "' references unknown cluster '" cluster "'")
                        {:observer (:name observer)
                         :cluster  cluster
                         :known    known-clusters})))))
  config)

(defn parse-config [raw]
  (-> (s/validate Config raw)
      validate-cluster-references))
