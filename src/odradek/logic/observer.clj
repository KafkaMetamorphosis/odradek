(ns odradek.logic.observer
  (:import [java.nio ByteBuffer]
           [java.util Random]))

(defn encode-payload
  "Returns a byte array of (* message-size-kb 1024) bytes.
   First 8 bytes: big-endian epoch-ms long.
   Remaining bytes: random padding."
  [timestamp-ms message-size-kb]
  (let [total-bytes (* message-size-kb 1024)
        buf         (byte-array total-bytes)]
    (doto (ByteBuffer/wrap buf)
      (.putLong timestamp-ms))
    (when (> total-bytes 8)
      (let [rng     (Random.)
            padding (byte-array (- total-bytes 8))]
        (.nextBytes rng padding)
        (System/arraycopy padding 0 buf 8 (alength padding))))
    buf))

(defn decode-produced-at-header
  "Extracts the produced-at epoch-ms from the com.franz.odradek/produced-at
   Kafka header. Returns nil if the header is absent."
  [^org.apache.kafka.clients.consumer.ConsumerRecord record]
  (when-let [header (-> record .headers (.lastHeader "com.franz.odradek/produced-at"))]
    (Long/parseLong (String. (.value header) "UTF-8"))))

(defn observer-group-id
  "Returns the consumer group ID: user-provided group.id override,
   or UPPERCASE(observer-name) when not overridden."
  [observer-name consumer-config]
  (or (get consumer-config "group.id")
      (.toUpperCase ^String observer-name)))

(defn derive-labels
  "Returns the 5 metric label values map for an observer x cluster pair."
  [observer cluster-name]
  {:cluster_name             cluster-name
   :observer                 (:name observer)
   :topic                    (:topic observer)
   :message_size_kb          (str (:message-size-kb observer))
   :configured_rate_interval (str (:messages-per-bucket observer))})
