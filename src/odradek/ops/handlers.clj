(ns odradek.ops.handlers
  (:require [clojure.walk]
            [odradek.ops.health :as health]))

(defn- redact-config [config]
  (clojure.walk/postwalk
    (fn [node]
      (if (map-entry? node)
        (let [[key-name _value] node]
          (if (re-find #"password|secret|token|api-key" (name key-name))
            [key-name "***REDACTED***"]
            node))
        node))
    config))

(defn health-handler [{:keys [observer-statuses]} _request]
  (let [statuses (if observer-statuses @observer-statuses {})
        result   (health/run-checks statuses)]
    {:status (if (= :healthy (:status result)) 200 503)
     :body   result}))

(defn liveness-handler [_components _request]
  {:status 200 :body {:status "alive"}})

(defn readiness-handler [{:keys [observer-statuses]} _request]
  (let [statuses (if observer-statuses @observer-statuses {})]
    (if (health/ready? statuses)
      {:status 200 :body {:status "ready"}}
      {:status 503 :body {:status "not ready"}})))

(defn config-dump-handler [{:keys [config]} _request]
  {:status 200 :body (redact-config config)})
