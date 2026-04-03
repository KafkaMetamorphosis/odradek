(ns odradek.core
  (:require [com.stuartsierra.component :as component]
            [odradek.system :as system]
            [clojure.tools.logging :as log])
  (:gen-class))

(defn -main [& _args]
  (log/info "Starting Odradek...")
  (let [system (component/start (system/new-system))]
    (.addShutdownHook (Runtime/getRuntime)
                      (Thread. #(do (log/info "Shutting down Odradek...")
                                    (component/stop system))))
    (log/info "Odradek started.")))
