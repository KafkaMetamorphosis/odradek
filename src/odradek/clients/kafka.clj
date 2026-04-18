(ns odradek.clients.kafka
  (:import [org.apache.kafka.clients.producer KafkaProducer]
           [org.apache.kafka.clients.consumer KafkaConsumer]
           [org.apache.kafka.clients.admin AdminClient]
           [org.apache.kafka.common.serialization
            ByteArraySerializer ByteArrayDeserializer]))

(defn- ->properties [m]
  (let [props (java.util.Properties.)]
    (doseq [[k v] m]
      (.setProperty props (name k) (str v)))
    props))

;; change this, is coupled to log/config but should ge agnostic

(defn new-producer
  "Constructs a KafkaProducer with ByteArraySerializer.
   producer-config is a map of string keys to values (Kafka property names)."
  [bootstrap-url producer-config]
  (let [config (merge producer-config
                      {"bootstrap.servers" bootstrap-url
                       "key.serializer"    (.getName ByteArraySerializer)
                       "value.serializer"  (.getName ByteArraySerializer)})]
    (KafkaProducer. (->properties config))))

(defn new-consumer
  "Constructs a KafkaConsumer with ByteArrayDeserializer.
   Fixed: auto.offset.reset=latest, enable.auto.commit=false, max.poll.records=1.
   group.id must be present in consumer-config."
  [bootstrap-url consumer-config]
  (let [config (merge consumer-config
                      {"bootstrap.servers"  bootstrap-url
                       "key.deserializer"   (.getName ByteArrayDeserializer)
                       "value.deserializer" (.getName ByteArrayDeserializer)
                       "auto.offset.reset"  "latest"
                       "enable.auto.commit" "false"
                       "max.poll.records"   "1"})]
    (KafkaConsumer. (->properties config))))

(defn new-admin-client
  "Constructs a KafkaAdminClient connected to the given bootstrap URL."
  [bootstrap-url]
  (AdminClient/create (->properties {"bootstrap.servers" bootstrap-url})))
