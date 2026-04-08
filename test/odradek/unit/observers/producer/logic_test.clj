(ns odradek.unit.observers.producer.logic-test
  (:require [clojure.test :refer [deftest is testing]]
            [odradek.observers.producer.logic :as producer-logic])
  (:import [java.nio ByteBuffer]))

(deftest derive-labels-returns-all-producer-labels
  (testing "returns all standard label keys with correct values and nil custom-labels when absent"
    (let [observer {:name          "prod-obs"
                    :topic         "my-topic"
                    :volume-config {:message-size-kb      64
                                    :messages-per-interval 100}}
          labels   (producer-logic/derive-labels observer "cluster-a")]
      (is (= {:cluster_name             "cluster-a"
              :observer                 "prod-obs"
              :topic                    "my-topic"
              :message_size_kb          "64"
              :configured_rate_interval "100"
              :custom-labels            nil}
             labels))))

  (testing "passes raw custom-labels map through without transformation"
    (let [observer {:name          "prod-obs"
                    :topic         "my-topic"
                    :volume-config {:message-size-kb      64
                                    :messages-per-interval 100}
                    :custom-labels {:slo-latency-ms 20}}
          labels   (producer-logic/derive-labels observer "cluster-a")]
      (is (= {:slo-latency-ms 20} (:custom-labels labels))))))

(deftest encode-payload-size
  (testing "encoded payload has the correct byte length"
    (is (= (* 2 1024) (alength (producer-logic/encode-payload 123456789 2))))))

(deftest encode-payload-timestamp-roundtrip
  (testing "first 8 bytes decode back to original timestamp"
    (let [ts  1710000000123
          buf (producer-logic/encode-payload ts 1)]
      (is (= ts (.getLong (ByteBuffer/wrap buf)))))))
