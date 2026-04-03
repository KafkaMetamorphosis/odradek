(ns odradek.unit.wire.in-test
  (:require [clojure.test :refer [deftest is testing]]
            [odradek.wire.in :as in]))

(def valid-config
  {:observers       [{:name                "obs-1"
                      :clusters            ["cluster-a"]
                      :topic               "MY-TOPIC"
                      :parallelism         1
                      :messages-per-bucket 10
                      :message-size-kb     100
                      :producer-config     {"acks" "all"}
                      :consumer-config     {}}]
   :kafka_clusters  {"cluster-a" {:bootstrap-url "localhost:9092"}}
   :producer-engine {:rate-interval-ms 100}})

(deftest parse-config-valid
  (testing "valid config passes without throwing"
    (is (= valid-config (in/parse-config valid-config)))))

(deftest parse-config-missing-observers
  (testing "missing :observers throws"
    (is (thrown? Exception
          (in/parse-config (dissoc valid-config :observers))))))

(deftest parse-config-missing-parallelism
  (testing "missing :parallelism throws"
    (is (thrown? Exception
          (in/parse-config
            (assoc-in valid-config [:observers 0]
              (dissoc (first (:observers valid-config)) :parallelism)))))))

(deftest parse-config-unknown-cluster
  (testing "observer referencing unknown cluster throws"
    (is (thrown-with-msg? clojure.lang.ExceptionInfo #"unknown cluster"
          (in/parse-config
            (assoc-in valid-config [:observers 0 :clusters] ["nonexistent"]))))))
