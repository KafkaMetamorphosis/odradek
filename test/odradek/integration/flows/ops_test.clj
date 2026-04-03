(ns odradek.integration.flows.ops-test
  (:require [com.stuartsierra.component :as component]
            [state-flow.api :refer [defflow flow match?]]
            [odradek.helpers.system :as test-system]
            [odradek.helpers.http :as http]))

(defn- init-system []
  (test-system/build-test-system))

(defn- stop-system [system]
  (component/stop system))

(defflow liveness-returns-200
  {:init init-system :cleanup stop-system}
  (flow "GET /ops/liveness returns 200"
    (match? {:status 200 :body {:status "alive"}}
            (http/GET "/ops/liveness"))))

(defflow config-dump-returns-200
  {:init init-system :cleanup stop-system}
  (flow "GET /ops/config/dump returns 200"
    (match? {:status 200}
            (http/GET "/ops/config/dump"))))

(defflow readiness-probe
  {:init init-system :cleanup stop-system}
  (flow "GET /ops/readiness returns 200 or 503"
    (match? {:status (comp #{200 503} identity)}
            (http/GET "/ops/readiness"))))

(defflow health-probe
  {:init init-system :cleanup stop-system}
  (flow "GET /ops/health returns 200 or 503"
    (match? {:status (comp #{200 503} identity)}
            (http/GET "/ops/health"))))
