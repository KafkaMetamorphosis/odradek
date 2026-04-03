(ns odradek.controllers.metrics-handler
  (:require [odradek.components.metrics-registry :as metrics-registry]))

(defn metrics-handler [{:keys [metrics-registry]} _request]
  {:status  200
   :headers {"Content-Type" "text/plain; version=0.0.4; charset=utf-8"}
   :body    (metrics-registry/scrape metrics-registry)})
