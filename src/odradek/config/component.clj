(ns odradek.config.component
  (:require [cheshire.core :as json]
            [clojure.java.io :as io]
            [clojure.tools.logging :as log]
            [com.stuartsierra.component :as component]))

(defn- read-config-from-filesystem [config-file-path]
  (log/info "Loading config from filesystem path:" config-file-path)
  (let [config-file (io/file config-file-path)]
    (when-not (.exists config-file)
      (throw (ex-info "CONFIG_PATH is set but the file does not exist"
                      {:config-path config-file-path})))
    (json/parse-string (slurp config-file) true)))

(defn- read-config-from-classpath []
  (log/info "CONFIG_PATH not set — loading config from classpath resource config.json")
  (let [classpath-resource (io/resource "config.json")]
    (when-not classpath-resource
      (throw (ex-info "config.json not found on classpath and CONFIG_PATH is not set" {})))
    (json/parse-string (slurp classpath-resource) true)))

(defn- read-json-config []
  ;; (System/getenv "CONFIG_PATH") returns "" when the env var is set to an empty string
  ;; (e.g. the Dockerfile default `ENV CONFIG_PATH=""`). not-empty coerces "" to nil so
  ;; the classpath fallback is taken correctly in that case.
  (if-let [config-file-path (not-empty (System/getenv "CONFIG_PATH"))]
    (read-config-from-filesystem config-file-path)
    (read-config-from-classpath)))


(defrecord ConfigComponent [config]
  component/Lifecycle
  (start [this]
    (let [loaded-config (read-json-config)]
      (assoc this :config loaded-config)))
  (stop [this] this))

(defn new-config-component []
  (map->ConfigComponent {}))

(defrecord StubConfigComponent [config]
  component/Lifecycle
  (start [this] this)
  (stop [this] this))

(defn new-stub-config-component [config-map]
  (map->StubConfigComponent {:config config-map}))

