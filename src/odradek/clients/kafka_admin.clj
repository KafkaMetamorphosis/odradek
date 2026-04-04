(ns odradek.clients.kafka-admin
  (:import [org.apache.kafka.clients.admin AdminClient AdminClientConfig]
           [org.apache.kafka.common.config ConfigResource ConfigResource$Type]
           [java.util.concurrent TimeUnit]))

(defn new-admin-client
  "Creates a Kafka AdminClient connected to the given bootstrap URL."
  [bootstrap-url]
  (AdminClient/create
    {AdminClientConfig/BOOTSTRAP_SERVERS_CONFIG bootstrap-url}))

(defn describe-topics
  "Returns a map of topic-name string to TopicDescription for each topic."
  [admin-client topic-names]
  (let [result (-> admin-client
                   (.describeTopics topic-names)
                   (.allTopicNames)
                   (.get 10 TimeUnit/SECONDS))]
    (into {} result)))

(defn describe-topic-configs
  "Returns a map of topic-name string to Config for each topic.
   AdminClient.describeConfigs keys results by ConfigResource, so we
   extract the topic name from each resource to build a string-keyed map."
  [admin-client topic-names]
  (let [config-resources (map #(ConfigResource. ConfigResource$Type/TOPIC %) topic-names)
        result           (-> admin-client
                             (.describeConfigs config-resources)
                             (.all)
                             (.get 10 TimeUnit/SECONDS))]
    (into {}
      (map (fn [[config-resource config]]
             [(.name config-resource) config])
           result))))

(defn close-admin-client
  "Closes the AdminClient."
  [admin-client]
  (.close admin-client))
