(ns odradek.unit.logic.topic-config-test
  (:require [clojure.test :refer [deftest is testing]]
            [odradek.logic.topic-config :as topic-config]))

(deftest cluster-topic-pairs-extracts-all-combinations
  (testing "returns all cluster-topic pairs from observers"
    (let [observers [{:name "obs-1" :clusters ["c1" "c2"] :topic "T1"}
                     {:name "obs-2" :clusters ["c1"]      :topic "T2"}]]
      (is (= [["c1" "T1"] ["c2" "T1"] ["c1" "T2"]]
             (topic-config/cluster-topic-pairs observers))))))

(deftest cluster-topic-pairs-deduplicates
  (testing "duplicate cluster-topic pairs are removed"
    (let [observers [{:name "obs-1" :clusters ["c1"] :topic "T1"}
                     {:name "obs-2" :clusters ["c1"] :topic "T1"}]]
      (is (= [["c1" "T1"]]
             (topic-config/cluster-topic-pairs observers))))))

(deftest cluster-topic-pairs-handles-empty-observers
  (testing "empty observers returns empty"
    (is (= [] (topic-config/cluster-topic-pairs [])))))
