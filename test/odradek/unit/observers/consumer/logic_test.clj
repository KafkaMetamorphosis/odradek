(ns odradek.unit.observers.consumer.logic-test
  (:require [clojure.test :refer [deftest is testing]]
            [odradek.observers.consumer.logic :as consumer-logic]))

(deftest derive-labels-returns-consumer-labels
  (testing "returns consumer label keys with correct values and nil custom-labels when absent"
    (let [observer {:name  "cons-obs"
                    :topic "my-topic"}
          labels   (consumer-logic/derive-labels observer "cluster-b")]
      (is (= {:cluster_name  "cluster-b"
              :observer      "cons-obs"
              :topic         "my-topic"
              :custom-labels nil}
             labels))))

  (testing "passes raw custom-labels map through without transformation"
    (let [observer {:name          "cons-obs"
                    :topic         "my-topic"
                    :custom-labels {:slo-latency-ms 20}}
          labels   (consumer-logic/derive-labels observer "cluster-b")]
      (is (= {:slo-latency-ms 20} (:custom-labels labels))))))

(deftest observer-group-id-derived
  (testing "returns uppercase observer name when no override"
    (is (= "MY-OBSERVER" (consumer-logic/observer-group-id "my-observer" {})))))

(deftest observer-group-id-override
  (testing "returns override when group.id is in consumer-config"
    (is (= "CUSTOM_GROUP" (consumer-logic/observer-group-id "my-observer" {"group.id" "CUSTOM_GROUP"})))))
