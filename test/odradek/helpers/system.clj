(ns odradek.helpers.system
  (:require [com.stuartsierra.component :as component]
            [odradek.system :as system]))

(defn build-test-system []
  (component/start (system/new-system :test)))
