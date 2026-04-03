(ns odradek.unit.logic.observer-test
  (:require [clojure.test :refer [deftest is testing]]
            [odradek.logic.observer :as observer])
  (:import [java.nio ByteBuffer]))

(deftest encode-payload-size
  (testing "encoded payload has the correct byte length"
    (is (= (* 2 1024) (alength (observer/encode-payload 123456789 2))))))

(deftest encode-payload-timestamp-roundtrip
  (testing "first 8 bytes decode back to original timestamp"
    (let [ts  1710000000123
          buf (observer/encode-payload ts 1)]
      (is (= ts (.getLong (ByteBuffer/wrap buf)))))))

(deftest observer-group-id-derived
  (testing "returns uppercase observer name when no override"
    (is (= "MY-OBSERVER" (observer/observer-group-id "my-observer" {})))))

(deftest observer-group-id-override
  (testing "returns override when group.id is in consumer-config"
    (is (= "CUSTOM_GROUP" (observer/observer-group-id "my-observer" {"group.id" "CUSTOM_GROUP"})))))

(deftest derive-labels-keys
  (testing "returns all 5 expected label keys"
    (let [obs    {:name "obs" :topic "T" :message-size-kb 100 :messages-per-bucket 10}
          labels (observer/derive-labels obs "clust")]
      (is (= #{:cluster_name :observer :topic :message_size_kb :configured_rate_interval}
             (set (keys labels)))))))
