(ns odradek.ops.handlers
  (:require [cheshire.core :as json]
            [clojure.walk]
            [odradek.ops.health :as health]))

(defn- json-response [status body]
  {:status  status
   :headers {"Content-Type" "application/json"}
   :body    (json/generate-string body)})

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
    (json-response (if (= :healthy (:status result)) 200 503) result)))

(defn liveness-handler [_components _request]
  (json-response 200 {:status "alive"}))

(defn readiness-handler [_components _request]
  (json-response 200 {:status "ready"}))

(defn config-dump-handler [{:keys [config]} _request]
  (json-response 200 (redact-config config)))
