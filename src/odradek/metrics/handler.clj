(ns odradek.metrics.handler
  (:require [odradek.metrics.registry :as m-registry]))

(defn metrics-handler [{:keys [metrics-registry]} _request]
  {:status  200
   :headers {"Content-Type" "text/plain; version=0.0.4; charset=utf-8"}
   :body    (m-registry/scrape metrics-registry)})
