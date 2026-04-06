(ns odradek.observers.topic-info.component
  (:require [clojure.core.async :as async]
            [clojure.tools.logging :as log]
            [com.stuartsierra.component :as component]
            [odradek.clients.kafka-admin :as kafka-admin]
            [odradek.observers.topic-info.logic :as topic-config]
            [odradek.metrics.registry :as metrics]))

(defn- topics-for-cluster
  "Returns distinct topic names from producer/consumer observers that reference the given cluster."
  [all-observers cluster-name]
  (->> all-observers
       (filter #(and (:topic %) (some #{cluster-name} (:clusters %))))
       (map :topic)
       distinct))

(defn- build-admin-clients
  "Creates one AdminClient per cluster listed in the topic-info observer's clusters."
  [observer-clusters kafka-clusters]
  (into {}
    (for [cluster-name observer-clusters
          :let [cluster-config (get kafka-clusters cluster-name)]]
      [cluster-name (kafka-admin/new-admin-client (:bootstrap-url cluster-config))])))

(defn- scrape-cluster
  "Scrapes topic descriptions and configs for the given topics on one cluster,
   updating the topic-config gauge for each."
  [admin-client cluster-name topics metrics-registry]
  (let [topic-descriptions (kafka-admin/describe-topics admin-client topics)
        topic-configs      (kafka-admin/describe-topic-configs admin-client topics)]
    (doseq [topic topics]
      (let [description (get topic-descriptions topic)
            config      (get topic-configs topic)
            label-map   (topic-config/build-label-values cluster-name topic description config)]
        (metrics/set-topic-config! metrics-registry label-map)))
    (log/debug "Scraped topic configs" {:cluster cluster-name :topic-count (count topics)})))

(defn- scrape-all-clusters
  "Scrapes all clusters for this observer. Runs on a real thread (blocking Kafka admin calls)."
  [admin-clients all-observers metrics-registry observer-name]
  (doseq [[cluster-name admin-client] admin-clients]
    (try
      (let [topics (topics-for-cluster all-observers cluster-name)]
        (when (seq topics)
          (scrape-cluster admin-client cluster-name topics metrics-registry)))
      (catch Exception exception
        (log/warn exception "Failed to scrape topic configs" {:cluster  cluster-name
                                                              :observer observer-name})))))

(defrecord TopicScraperComponent [observer kafka-clusters all-observers metrics-registry trigger-ch
                                  admin-clients]
  component/Lifecycle
  (start [this]
    (let [observer-clusters (:clusters observer)
          clients           (build-admin-clients observer-clusters kafka-clusters)]
      (async/go-loop []
        (when (async/<! trigger-ch)
          ;; Park the go thread while the blocking Kafka admin calls run on a real thread
          (async/<! (async/thread
                      (scrape-all-clusters clients all-observers metrics-registry (:name observer))))
          (recur)))
      (log/info "TopicScraper started" {:observer (:name observer)
                                        :clusters observer-clusters})
      (assoc this :admin-clients clients)))

  (stop [this]
    (log/info "Stopping TopicScraper..." {:observer (:name observer)})
    ;; trigger-ch is closed by the orchestrator before stop is called — that exits the go-loop
    (doseq [[cluster-name admin-client] admin-clients]
      (try
        (kafka-admin/close-admin-client admin-client)
        (catch Exception exception
          (log/warn exception "Failed to close AdminClient" {:cluster cluster-name}))))
    (log/info "TopicScraper stopped." {:observer (:name observer)})
    (assoc this :admin-clients nil)))

(defn new-topic-info-observer [observer kafka-clusters all-observers metrics-registry trigger-ch]
  (map->TopicScraperComponent
    {:observer          observer
     :kafka-clusters    kafka-clusters
     :all-observers     all-observers
     :metrics-registry  metrics-registry
     :trigger-ch        trigger-ch}))
