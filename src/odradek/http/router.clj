(ns odradek.http.router
  (:require [compojure.core :refer [GET POST PUT DELETE]]
            [compojure.core :as compojure]
            [odradek.ops.handlers :as ops-handlers]
            [odradek.metrics.handler :as m-handlers]
            [cheshire.core :as json]
            [clojure.tools.logging :as log]
            [com.stuartsierra.component :as component]
            [ring.middleware.keyword-params :refer [wrap-keyword-params]]
            [ring.middleware.params :refer [wrap-params]])
  (:import [org.slf4j MDC]
           [java.util UUID]))



;;(defn wrap-js;; on-body [handler]
  ;;   (fn [request]
  ;;     (let [body (when-let [body-stream (:body request)]
  ;;                  (when (and body-stream
  ;;                             (some-> (get-in request [:headers "content-type"])
  ;;                                     (.contains "application/json")))
  ;;                    (json/parse-string (slurp body-stream) true)))]
  ;;       (handler (cond-> request
  ;;                  body (assoc :body body)))))
  
  
  ;;)
;; 
;; (defn wrap-json-response [handler]
;;   (fn [request]
;;     (let [response (handler request)]
;;       (when response
;;         (-> response
;;             (update :body json/generate-string)
;;             (assoc-in [:headers "Content-Type"] "application/json"))))))

(defn wrap-exception-handler [handler]
  (fn [request]
    (try
      (handler request)
      (catch Exception exception
        (log/error exception "Unhandled exception")
        {:status  500
         :headers {"Content-Type" "application/json"}
         :body    (json/generate-string {:error "internal server error"})}))))

(defn wrap-not-found [handler]
  (fn [request]
    (or (handler request)
        {:status 404
         :body   {:error "not found"}})))

(defn wrap-request-logging [handler]
  (fn [request]
    (let [method     (-> request :request-method name .toUpperCase)
          path       (:uri request)
          request-id (or (get-in request [:headers "x-request-id"])
                         (str (UUID/randomUUID)))
          start-time (System/currentTimeMillis)]
      (try
        (MDC/put "request-id" request-id)
        (MDC/put "method" method)
        (MDC/put "path" path)
        (log/info (str "HTTP " method " " path))
        (let [response (handler request)
              duration (- (System/currentTimeMillis) start-time)]
          (log/info (str "HTTP " method " " path " -> " (or (:status response) "nil") " (" duration "ms)"))
          response)
        (finally
          (MDC/clear))))))

(defmacro compojure-route [method path handler]
  (let [request-sym (gensym "request")]
    `(case ~method
       "GET"    (GET    ~path ~request-sym (~handler ~request-sym))
       "POST"   (POST   ~path ~request-sym (~handler ~request-sym))
       "PUT"    (PUT    ~path ~request-sym (~handler ~request-sym))
       "DELETE" (DELETE ~path ~request-sym (~handler ~request-sym)))))

(def ^:private routes-manifest
  [["GET" "/ops/health"      ops-handlers/health-handler]
   ["GET" "/ops/liveness"    ops-handlers/liveness-handler]
   ["GET" "/ops/readiness"   ops-handlers/readiness-handler]
   ["GET" "/ops/config/dump" ops-handlers/config-dump-handler]
   ["GET" "/metrics"         m-handlers/metrics-handler]])

(defn routes [components]
  (apply compojure/routes
         (map (fn [[method path handler]]
                (compojure-route method path (partial handler components)))
              routes-manifest)))

(defrecord RouterComponent [config metrics-registry observer-orchestrator handler]
  component/Lifecycle
  (start [this]
    (let [components {:config            (:config config)
                      :metrics-registry  metrics-registry
                      :observer-statuses (:observer-statuses observer-orchestrator)}
          routers (-> (routes components) 
                          wrap-not-found
                          wrap-keyword-params
                          wrap-params
                          wrap-exception-handler
                          wrap-request-logging)
          handler (compojure/routes routers)]
      (assoc this :handler handler)))
  (stop [this]
    (assoc this :handler nil)))

(defn new-router []
  (map->RouterComponent {}))
