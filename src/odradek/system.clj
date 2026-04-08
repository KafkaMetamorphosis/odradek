(ns odradek.system
  (:require [com.stuartsierra.component :as component]
            [odradek.config.component :as c-config]
            [odradek.http.server :as http-server]
            [odradek.http.router :as http-router]
            [odradek.metrics.registry :as metrics-registry]
            [odradek.observers.orchestrator :as observers-orchestrator]))

(defn new-system []
  (component/system-map
   :config                 (c-config/new-config-component)
   :metrics-registry       (component/using (metrics-registry/new-metrics-registry) [:config])
   :observer-orchestrator  (component/using (observers-orchestrator/new-observers-orchestrator) [:config :metrics-registry])
   :router                 (component/using (http-router/new-router) [:config :metrics-registry :observer-orchestrator])
   :http-server            (component/using (http-server/new-http-server) [:config :router])))

(defn new-system-with-config [config-map]
  (component/system-map
   :config                 (c-config/new-stub-config-component config-map)
   :metrics-registry       (component/using (metrics-registry/new-metrics-registry) [:config])
   :observer-orchestrator  (component/using (observers-orchestrator/new-observers-orchestrator) [:config :metrics-registry])
   :router                 (component/using (http-router/new-router) [:config :metrics-registry :observer-orchestrator])
   :http-server            (component/using (http-server/new-http-server) [:config :router])))
