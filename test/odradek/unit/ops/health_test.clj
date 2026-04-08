(ns odradek.unit.ops.health-test
  (:require [clojure.test :refer [deftest is testing]]
            [odradek.ops.health :as health]))

(deftest run-checks-healthy
  (testing "all :running -> :healthy"
    (let [statuses {[:obs :cluster :producer] {:status :running :since 0}
                    [:obs :cluster :consumer] {:status :running :since 0}}]
      (is (= :healthy (:status (health/run-checks statuses)))))))

(deftest run-checks-stopped
  (testing "any :stopped -> :unhealthy"
    (let [statuses {[:obs :cluster :producer] {:status :stopped :since 0}}]
      (is (= :unhealthy (:status (health/run-checks statuses)))))))

(deftest run-checks-long-backoff
  (testing "backoff longer than 5 minutes -> :unhealthy"
    (let [six-minutes-ago (- (System/currentTimeMillis) (* 6 60 1000))
          statuses        {[:obs :cluster :producer] {:status :backoff :since six-minutes-ago}}]
      (is (= :unhealthy (:status (health/run-checks statuses)))))))

(deftest run-checks-recent-backoff-ok
  (testing "backoff shorter than 5 minutes -> still :healthy"
    (let [recent-since (- (System/currentTimeMillis) 30000)
          statuses     {[:obs :cluster :producer] {:status :backoff :since recent-since}}]
      (is (= :healthy (:status (health/run-checks statuses)))))))
