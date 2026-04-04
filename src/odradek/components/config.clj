(ns odradek.components.config
  (:require [cheshire.core :as json]
            [clojure.java.io :as io]
            [com.stuartsierra.component :as component]))

(defn- keywordize-observer
  "Keywordizes structural keys in an observer map while preserving
   producer-config and consumer-config as string-keyed maps."
  [observer]
  (-> (into {} (map (fn [[k v]] [(keyword k) v])) observer)
      (update :clusters vec)))

(defn- keywordize-cluster-config
  "Keywordizes the values of each cluster (e.g. bootstrap-url) but keeps
   the cluster name keys as strings."
  [clusters]
  (into {}
        (map (fn [[cluster-name cluster-config]]
               [cluster-name
                (into {} (map (fn [[k v]] [(keyword k) v])) cluster-config)]))
        clusters))

(defn- keywordize-config
  "Converts JSON-parsed string-keyed config into the keyword-keyed
   structure the application expects, preserving string keys inside
   producer-config and consumer-config maps."
  [raw]
  {:server          {:port (get-in raw ["server" "port"])}
   :producer-engine {:rate-interval-ms (get-in raw ["producer-engine" "rate-interval-ms"])}
   :topic-scraper   {:scrape-interval-ms (get-in raw ["topic-scraper" "scrape-interval-ms"])}
   :kafka_clusters  (keywordize-cluster-config (get raw "kafka_clusters"))
   :observers       (mapv keywordize-observer (get raw "observers"))})

(defn- read-json-config []
  (let [raw (slurp (io/resource "config.json"))]
    (keywordize-config (json/parse-string raw))))

(defn- apply-env-overrides [config]
  (let [env-port               (System/getenv "PORT")
        env-bootstrap-url      (System/getenv "KAFKA_BOOTSTRAP_URL")
        env-rate-interval      (System/getenv "RATE_INTERVAL_MS")
        env-scrape-interval    (System/getenv "TOPIC_SCRAPE_INTERVAL_MS")]
    (cond-> config
      env-port
      (assoc-in [:server :port] (Long/parseLong env-port))

      env-bootstrap-url
      (update :kafka_clusters
              (fn [clusters]
                (into {}
                      (map (fn [[cluster-name cluster-config]]
                             [cluster-name (assoc cluster-config :bootstrap-url env-bootstrap-url)]))
                      clusters)))

      env-rate-interval
      (assoc-in [:producer-engine :rate-interval-ms] (Long/parseLong env-rate-interval))

      env-scrape-interval
      (assoc-in [:topic-scraper :scrape-interval-ms] (Long/parseLong env-scrape-interval)))))

(defrecord ConfigComponent [config]
  component/Lifecycle
  (start [this]
    (let [loaded-config (-> (read-json-config)
                            apply-env-overrides)]
      (assoc this :config loaded-config)))
  (stop [this] this))

(defrecord StubConfigComponent [config]
  component/Lifecycle
  (start [this] this)
  (stop [this] this))

(defn new-config-component []
  (map->ConfigComponent {}))

(defn new-stub-config-component [config-map]
  (map->StubConfigComponent {:config config-map}))
