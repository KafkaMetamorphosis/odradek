(ns odradek.unit.wire.in-test
  (:require [clojure.test :refer [deftest is testing]]
            [odradek.config.schema :as schema]))

(def valid-config
  {:observers           [{:name            "obs-producer"
                          :clusters        ["cluster-a"]
                          :topic           "MY-TOPIC"
                          :observer-type   :producer
                          :volume-config   {:parallelism         1
                                            :messages-per-interval 10
                                            :message-size-kb     100}
                          :producer-config {"acks" "all"}}
                         {:name            "obs-consumer"
                          :clusters        ["cluster-a"]
                          :topic           "MY-TOPIC"
                          :observer-type   :consumer
                          :consumer-config {}}]
   :kafka_clusters      {"cluster-a" {:bootstrap-url "localhost:9092"}}
   :orchestrator-config {:rate-interval-ms   100
                         :scrape-interval-ms 30000}})

(deftest parse-config-valid
  (testing "valid config passes without throwing"
    (is (= valid-config (schema/parse-config valid-config)))))

(deftest parse-config-missing-observers
  (testing "missing :observers throws"
    (is (thrown? Exception
          (schema/parse-config (dissoc valid-config :observers))))))

(deftest parse-config-unknown-cluster
  (testing "observer referencing unknown cluster throws"
    (is (thrown-with-msg? clojure.lang.ExceptionInfo #"Errors found"
          (schema/parse-config
            (assoc-in valid-config [:observers 0 :clusters] ["nonexistent"]))))))
