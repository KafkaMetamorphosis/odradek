(ns odradek.observers.consumer.logic)

(defn derive-labels
  "Returns the metric label values map for a consumer observer x cluster pair."
  [observer cluster-name]
  {:cluster_name cluster-name
   :observer     (:name observer)
   :topic        (:topic observer)})

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
