(ns odradek.integration.flows.observer-engine-test
  (:require [clojure.test :refer [is]]
            [state-flow.api :refer [defflow flow]]
            [odradek.helpers.system :as test-system]))

(defn- init-system []
  (test-system/build-test-system))

(defflow observer-statuses-initialized-on-start
  {:init init-system}
  (flow "observer-statuses atom has entries after start"
    [system (state-flow.api/get-state)]
    [:let [statuses @(-> system :observer-engine :observer-statuses)]]
    (state-flow.api/return
      (is (pos? (count statuses))))))

(defflow observer-statuses-contain-expected-keys
  {:init init-system}
  (flow "observer-statuses contains producer and consumer entries for test-observer"
    [system (state-flow.api/get-state)]
    [:let [statuses @(-> system :observer-engine :observer-statuses)
           keys     (set (keys statuses))]]
    (state-flow.api/return
      (do
        (is (contains? keys ["test-observer" "test-cluster" :producer]))
        (is (contains? keys ["test-observer" "test-cluster" :consumer]))))))
