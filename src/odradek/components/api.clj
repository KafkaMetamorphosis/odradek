(ns odradek.components.api
  (:require [com.stuartsierra.component :as component]
            [compojure.core :as compojure]
            [odradek.controllers.router :as router]
            [odradek.controllers.middleware :as middleware]
            [ring.middleware.keyword-params :refer [wrap-keyword-params]]
            [ring.middleware.params :refer [wrap-params]]))

(defrecord ApiComponent [config metrics-registry observer-engine handler]
  component/Lifecycle
  (start [this]
    (let [components {:config            (:config config)
                      :metrics-registry  metrics-registry
                      :observer-statuses (:observer-statuses observer-engine)}
          ops-handler (-> (router/ops-routes components)
                          middleware/wrap-not-found
                          middleware/wrap-json-body
                          middleware/wrap-json-response
                          wrap-keyword-params
                          wrap-params
                          middleware/wrap-exception-handler
                          middleware/wrap-request-logging)
          metrics-handler (router/metrics-route components)
          handler (compojure/routes metrics-handler ops-handler)]
      (assoc this :handler handler)))
  (stop [this]
    (assoc this :handler nil)))

(defn new-api []
  (map->ApiComponent {}))
