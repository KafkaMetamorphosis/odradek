(ns odradek.controllers.router
  (:require [compojure.core :refer [GET POST PUT DELETE]]
            [compojure.core :as compojure]
            [odradek.ops.handlers :as ops-handlers]
            [odradek.controllers.metrics-handler :as metrics-handler]))

(defmacro compojure-route [method path handler]
  (let [request-sym (gensym "request")]
    `(case ~method
       "GET"    (GET    ~path ~request-sym (~handler ~request-sym))
       "POST"   (POST   ~path ~request-sym (~handler ~request-sym))
       "PUT"    (PUT    ~path ~request-sym (~handler ~request-sym))
       "DELETE" (DELETE ~path ~request-sym (~handler ~request-sym)))))

(def ^:private ops-route-manifest
  [["GET" "/ops/health"      ops-handlers/health-handler]
   ["GET" "/ops/liveness"    ops-handlers/liveness-handler]
   ["GET" "/ops/readiness"   ops-handlers/readiness-handler]
   ["GET" "/ops/config/dump" ops-handlers/config-dump-handler]])

(defn ops-routes [components]
  (apply compojure/routes
    (map (fn [[method path handler]]
           (compojure-route method path (partial handler components)))
         ops-route-manifest)))

(defn metrics-route [components]
  (GET "/metrics" request
    (metrics-handler/metrics-handler components request)))

(defn routes [components]
  (compojure/routes
    (metrics-route components)
    (ops-routes components)))
