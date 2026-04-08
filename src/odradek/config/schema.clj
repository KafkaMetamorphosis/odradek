(ns odradek.config.schema
  (:require [schema.core :as s]))

(def ProducerConfig {s/Str s/Any})
(def ConsumerConfig {s/Str s/Any})
(def CustomLabels   {s/Str s/Any})

(def VolumeConfig
  {:parallelism s/Int
   :messages-per-interval s/Int
   :message-size-kb s/Int})

(def Observer
  {:name                                 s/Str
   :clusters                             [s/Str]
   :observer-type                        (s/enum :topic-info :producer :consumer)
   (s/optional-key :topic)               s/Str
   (s/optional-key :volume-config)       VolumeConfig
   (s/optional-key :custom-labels)       CustomLabels
   (s/optional-key :producer-config)     ProducerConfig
   (s/optional-key :consumer-config)     ConsumerConfig})

(def KafkaCluster
  {:bootstrap-url s/Str})

(def OrchestratorConfig
  {:rate-interval-ms   s/Int
   :scrape-interval-ms s/Int})

(def Config
  {:observers           [Observer]
   :kafka_clusters      {s/Str KafkaCluster}
   :orchestrator-config OrchestratorConfig})

(defn- validate-cluster-refs
  "Returns a sequence of error strings for observers referencing unknown clusters."
  [config]
  (let [known-clusters (set (keys (:kafka_clusters config)))]
    (->> (:observers config)
         (mapcat (fn [observer]
                   (keep (fn [cluster-name]
                           (when-not (contains? known-clusters cluster-name)
                             (str "Observer '" (:name observer) "' references unknown cluster '" cluster-name "'")))
                         (:clusters observer)))))))

(defn- validate-observer-required-keys
  "Returns a sequence of error strings for observers of the given type
   that are missing any of the required keys."
  [config observer-type required-keys]
  (->> (:observers config)
       (filter #(= (:observer-type %) observer-type))
       (mapcat (fn [observer]
                 (keep (fn [required-key]
                         (when (nil? (get observer required-key))
                           (str required-key " is mandatory for observer-type=" (name observer-type)
                                " at observer=" (:name observer))))
                       required-keys)))))

(defn- validate-config
  "Runs all validation rules against the parsed config. Throws if any errors found."
  [config]
  (let [errors (->> (concat
                      (validate-cluster-refs config)
                      (validate-observer-required-keys config :producer [:volume-config :producer-config])
                      (validate-observer-required-keys config :consumer [:consumer-config]))
                    (remove nil?)
                    vec)]
    (when (seq errors)
      (throw (ex-info "Errors found in the config.json" {:errors errors})))
    config))

(defn parse-config [raw]
  (-> (s/validate Config raw)
      validate-config))
