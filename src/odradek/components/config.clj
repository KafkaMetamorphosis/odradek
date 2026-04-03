(ns odradek.components.config
  (:require [aero.core :as aero]
            [clojure.java.io :as io]
            [com.stuartsierra.component :as component]))

(defrecord ConfigComponent [profile config]
  component/Lifecycle
  (start [this]
    (let [config (aero/read-config (io/resource "config.edn") {:profile profile})]
      (assoc this :config config)))
  (stop [this] this))

(defn new-config-component [profile]
  (map->ConfigComponent {:profile profile}))
