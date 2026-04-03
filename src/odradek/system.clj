(ns odradek.system
  (:require [com.stuartsierra.component :as component]
            [odradek.components.config :as config]
            [odradek.components.api :as api]
            [odradek.components.http-server :as http-server]
            [odradek.components.metrics-registry :as metrics-registry]
            [odradek.components.observer-engine :as observer-engine]))

(defn new-system
  ([] (new-system :default))
  ([profile]
   (component/system-map
     :config           (config/new-config-component profile)
     :metrics-registry (metrics-registry/new-metrics-registry)
     :observer-engine  (component/using (observer-engine/new-observer-engine)
                         [:config :metrics-registry])
     :api              (component/using (api/new-api)
                         [:config :metrics-registry :observer-engine])
     :http-server      (component/using (http-server/new-http-server)
                         [:config :api]))))
