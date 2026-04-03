(ns odradek.clients.kafka
  (:require [odradek.logic.observer :as observer])
  (:import [org.apache.kafka.clients.producer KafkaProducer]
           [org.apache.kafka.clients.consumer KafkaConsumer]
           [org.apache.kafka.common.serialization
            ByteArraySerializer ByteArrayDeserializer]))

(defn- ->properties [m]
  (let [props (java.util.Properties.)]
    (doseq [[k v] m]
      (.setProperty props (str k) (str v)))
    props))

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
   group.id derived from observer-name unless overridden in consumer-config."
  [bootstrap-url consumer-config observer-name]
  (let [group-id (observer/observer-group-id observer-name consumer-config)
        config   (merge consumer-config
                        {"bootstrap.servers"  bootstrap-url
                         "key.deserializer"   (.getName ByteArrayDeserializer)
                         "value.deserializer" (.getName ByteArrayDeserializer)
                         "auto.offset.reset"  "latest"
                         "enable.auto.commit" "false"
                         "max.poll.records"   "1"
                         "group.id"           group-id})]
    (KafkaConsumer. (->properties config))))
