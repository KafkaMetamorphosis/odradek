(ns odradek.http.server
  (:require [ring.adapter.jetty :as jetty]
            [com.stuartsierra.component :as component]
            [clojure.tools.logging :as log]))

(defrecord HttpServerComponent [config router server]
  component/Lifecycle
  (start [this]
    (let [port (get-in config [:config :server :port])
          handler (:handler router)
          server (jetty/run-jetty handler {:port port :join? false})]
      (log/infof "HTTP server started on port %d" port)
      (assoc this :server server)))
  (stop [this]
    (when server
      (.stop server)
      (log/info "HTTP server stopped"))
    (assoc this :server nil)))

(defn new-http-server []
  (map->HttpServerComponent {}))
