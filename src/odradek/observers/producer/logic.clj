(ns odradek.observers.producer.logic
  (:import [java.nio ByteBuffer]
           [java.util Random]))

(defn derive-labels
  "Returns the metric label values map for a producer observer x cluster pair.
   Includes message_size_kb and configured_rate_interval from volume-config.
   Includes :custom-labels as-is from the observer map (raw, no transformation)."
  [observer cluster-name]
  {:cluster_name             cluster-name
   :observer                 (:name observer)
   :topic                    (:topic observer)
   :message_size_kb          (str (get-in observer [:volume-config :message-size-kb]))
   :configured_rate_interval (str (get-in observer [:volume-config :messages-per-interval]))
   :custom-labels            (:custom-labels observer)})

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
