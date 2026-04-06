(ns odradek.integration.flows.observer-orchestrator-test
  (:require [clojure.test :refer [is]]
            [com.stuartsierra.component :as component]
            [state-flow.api :refer [defflow flow]]
            [odradek.helpers.system :as test-system]))

(defn- init-system []
  (test-system/build-test-system))

(defn- stop-system [system]
  (component/stop system))

(defflow started-observers-populated-on-start
  {:init init-system :cleanup stop-system}
  (flow "started-observers map has entries after orchestrator start"
    [system (state-flow.api/get-state)]
    [:let [started (-> system :observer-orchestrator :started-observers)]]
    (state-flow.api/return
      (is (pos? (count started))))))

(defflow started-observers-contain-expected-keys
  {:init init-system :cleanup stop-system}
  (flow "started-observers contains test-producer, test-consumer, and test-topic-info entries"
    [system (state-flow.api/get-state)]
    [:let [started (-> system :observer-orchestrator :started-observers)
           observer-names (set (keys started))]]
    (state-flow.api/return
      (do
        (is (contains? observer-names "test-producer"))
        (is (contains? observer-names "test-consumer"))
        (is (contains? observer-names "test-topic-info"))))))
