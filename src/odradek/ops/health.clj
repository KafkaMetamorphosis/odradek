(ns odradek.ops.health)

(def ^:private backoff-threshold-ms (* 5 60 1000))


(defn- backoff-too-long? [{:keys [status since]}]
  (and (= :backoff status)
       (> (- (System/currentTimeMillis) since) backoff-threshold-ms)))

(defn run-checks
  "Returns {:status :healthy/:unhealthy :checks {...}} based on observer loop statuses."
  [observer-statuses]
  (let [stopped?     (some #(= :stopped (:status %)) (vals observer-statuses))
        long-backoff? (some backoff-too-long? (vals observer-statuses))
        healthy?     (not (or stopped? long-backoff?))]
    {:status (if healthy? :healthy :unhealthy)
     :checks {:all-loops-active  (not stopped?)
              :no-prolonged-backoff (not long-backoff?)}}))
