(ns odradek.observers.topic-info.config-keys)

;; Complete list of Kafka topic-level configuration keys exposed as labels
;; on the kafka_odradek_topic_config Prometheus gauge.
;;
;; Two deprecated keys are intentionally included for compatibility with
;; brokers older than Kafka 3.6 (KIP-937):
;;   - message.format.version     (deprecated since 3.0, removed in future)
;;   - message.timestamp.difference.max.ms (deprecated since 3.0, superseded
;;     by message.timestamp.before.max.ms and message.timestamp.after.max.ms)
(def exposed-topic-config-keys
  ["cleanup.policy"
   "compression.type"
   "delete.retention.ms"
   "file.delete.delay.ms"
   "flush.messages"
   "flush.ms"
   "follower.replication.throttled.replicas"
   "index.interval.bytes"
   "leader.replication.throttled.replicas"
   "local.retention.bytes"
   "local.retention.ms"
   "max.compaction.lag.ms"
   "max.message.bytes"
   "message.downconversion.enable"
   "message.format.version"
   "message.timestamp.after.max.ms"
   "message.timestamp.before.max.ms"
   "message.timestamp.difference.max.ms"
   "message.timestamp.type"
   "min.cleanable.dirty.ratio"
   "min.compaction.lag.ms"
   "min.insync.replicas"
   "preallocate"
   "remote.storage.enable"
   "retention.bytes"
   "retention.ms"
   "segment.bytes"
   "segment.index.bytes"
   "segment.jitter.ms"
   "segment.ms"
   "unclean.leader.election.enable"])
