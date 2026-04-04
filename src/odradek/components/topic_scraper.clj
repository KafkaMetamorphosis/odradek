(ns odradek.components.topic-scraper
  (:require [clojure.tools.logging :as log]
            [com.stuartsierra.component :as component]
            [odradek.clients.kafka-admin :as kafka-admin]
            [odradek.logic.topic-config :as topic-config]
            [odradek.components.metrics-registry :as metrics]))

(defn- topics-for-cluster
  "Returns the distinct set of topic names observed on the given cluster."
  [observers cluster-name]
  (distinct
    (for [observer observers
          obs-cluster (:clusters observer)
          :when (= obs-cluster cluster-name)]
      (:topic observer))))

(defn- unique-cluster-names
  "Returns the distinct cluster names referenced across all observers."
  [observers]
  (distinct
    (mapcat :clusters observers)))

(defn- build-admin-clients
  "Creates one AdminClient per unique cluster referenced in observers."
  [observers kafka-clusters]
  (into {}
    (for [cluster-name (unique-cluster-names observers)
          :let [cluster-config (get kafka-clusters cluster-name)]]
      [cluster-name (kafka-admin/new-admin-client (:bootstrap-url cluster-config))])))

(defn- scrape-cluster
  "Scrapes topic descriptions and configs for all observed topics on one cluster,
   updating the topic-config-info gauge for each topic."
  [admin-client cluster-name topics metrics-registry]
  (let [topic-descriptions (kafka-admin/describe-topics admin-client topics)
        topic-configs      (kafka-admin/describe-topic-configs admin-client topics)]
    (doseq [topic topics]
      (let [description (get topic-descriptions topic)
            config      (get topic-configs topic)
            label-map   (topic-config/build-label-values cluster-name topic description config)]
        (metrics/set-topic-config! metrics-registry label-map)))
    (log/debug "Scraped topic configs" {:cluster cluster-name :topics (count topics)})))

(defn- scrape-loop
  "Periodically scrapes topic configs from all clusters until running? is false."
  [running? admin-clients observers metrics-registry scrape-interval-ms]
  (while @running?
    (doseq [[cluster-name admin-client] admin-clients]
      (try
        (let [topics (topics-for-cluster observers cluster-name)]
          (scrape-cluster admin-client cluster-name topics metrics-registry))
        (catch Exception exception
          (log/warn exception "Failed to scrape topic configs" {:cluster cluster-name}))))
    (Thread/sleep (long scrape-interval-ms))))

(defrecord TopicScraperComponent [config metrics-registry
                                  running? admin-clients scrape-thread]
  component/Lifecycle
  (start [this]
    (let [raw-config         (:config config)
          observers          (:observers raw-config)
          kafka-clusters     (:kafka_clusters raw-config)
          scrape-interval-ms (get-in raw-config [:topic-scraper :scrape-interval-ms])
          running-atom       (atom true)
          clients            (build-admin-clients observers kafka-clusters)
          thread             (doto (Thread.
                               #(scrape-loop running-atom clients observers metrics-registry scrape-interval-ms))
                               (.setName "odradek-topic-scraper")
                               (.setDaemon true)
                               .start)]
      (log/info "TopicScraper started" {:clusters (keys clients)
                                        :scrape-interval-ms scrape-interval-ms})
      (assoc this
        :running?      running-atom
        :admin-clients clients
        :scrape-thread thread)))

  (stop [this]
    (log/info "Stopping TopicScraper...")
    (when running?
      (reset! running? false))
    (when scrape-thread
      (.interrupt ^Thread scrape-thread)
      (.join ^Thread scrape-thread 10000))
    (doseq [[cluster-name admin-client] admin-clients]
      (try
        (kafka-admin/close-admin-client admin-client)
        (catch Exception exception
          (log/warn exception "Failed to close AdminClient" {:cluster cluster-name}))))
    (log/info "TopicScraper stopped.")
    (assoc this
      :running?      nil
      :admin-clients nil
      :scrape-thread nil)))

(defn new-topic-scraper []
  (map->TopicScraperComponent {}))
