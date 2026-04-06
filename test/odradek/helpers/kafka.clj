(ns odradek.helpers.kafka
  (:require [clj-http.client :as http]
            [clojure.string :as str])
  (:import [org.apache.kafka.clients.producer KafkaProducer ProducerRecord]
           [org.apache.kafka.common.serialization ByteArraySerializer]))

(defn- ->properties [m]
  (let [props (java.util.Properties.)]
    (doseq [[k v] m]
      (.setProperty props (str k) (str v)))
    props))

(defn produce-message!
  "Produces a single message to the given topic and closes the producer."
  [bootstrap-url topic ^bytes payload]
  (let [producer (KafkaProducer.
                   (->properties {"bootstrap.servers" bootstrap-url
                                  "key.serializer"    (.getName ByteArraySerializer)
                                  "value.serializer"  (.getName ByteArraySerializer)}))]
    (try
      @(.send producer (ProducerRecord. topic payload))
      (finally
        (.close producer)))))

(defn wait-for-metric
  "Polls GET /metrics on the given port until metric-name appears in the body,
   or timeout-ms elapses. Returns true if found, false on timeout."
  [port metric-name timeout-ms]
  (let [deadline (+ (System/currentTimeMillis) timeout-ms)]
    (loop []
      (let [resp (try
                   (http/get (str "http://localhost:" port "/metrics")
                             {:throw-exceptions false :as :text})
                   (catch Exception _ nil))
            body (some-> resp :body)]
        (cond
          (and body (str/includes? body metric-name)) true
          (> (System/currentTimeMillis) deadline) false
          :else (do (Thread/sleep 500) (recur)))))))

(defn scrape-metrics
  "Fetches GET /metrics and returns the response body string."
  [port]
  (let [response (http/get (str "http://localhost:" port "/metrics")
                           {:throw-exceptions false :as :text})]
    (:body response)))

(defn parse-metric-line
  "Finds a Prometheus exposition line matching metric-name (and optional
   filter substring), then parses its labels and numeric value.
   Returns {:labels {\"key\" \"value\" ...} :value 1.0} or nil if not found."
  ([body metric-name]
   (parse-metric-line body metric-name nil))
  ([body metric-name filter-string]
   (let [matching-line (->> (str/split-lines body)
                            (filter #(str/starts-with? % (str metric-name "{")))
                            (filter #(if filter-string
                                       (str/includes? % filter-string)
                                       true))
                            first)]
     (when matching-line
       (let [labels-str   (second (re-find #"\{(.+)\}" matching-line))
             value-str    (str/trim (subs matching-line (inc (.indexOf matching-line "}"))))
             parsed-value (Double/parseDouble value-str)
             labels       (into {}
                            (map (fn [pair]
                                   (let [[label-key label-value] (str/split pair #"=" 2)]
                                     [label-key (str/replace label-value #"\"" "")]))
                                 (re-seq #"[a-z_]+=(?:\"[^\"]*\")" labels-str)))]
         {:labels labels :value parsed-value})))))
