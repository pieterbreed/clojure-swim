(ns swim.core
  (:require [clojure.core.async :as async]))

(defn round
  "Naive implementation of rounding for floating point numbers. Returns an int"
  [fp]
  (let [sign (if (< 0 fp) 1 -1)]
    (-> fp
        (+ (* sign  0.5))
        int)))

(defn create-channel
  "Creates a channel based on an address"
  [address]
  nil)

(defn -init-cluster
  [cluster]
  (assoc cluster
    :channels (->> (get cluster :others [])
                   (map #(vector % ((:create-channel-fn cluster) %)))
                   (apply merge {}))))

(defn join-cluster
  "Creates a handle to a swim cluster using local id which must be unique to the cluster:
options:
{
:create-channel-fn function that will create a channel given an id
:k-factor value between 0 and 1 that will determine how many elements are chosen to send a ping-req to if a ping times out (default 0.67)
}
"
  ([my-address other-addresses options]
     (-init-cluster
      (merge {:create-channel-fn create-channel
              :k-factor 0.67}
             options 
            {:me my-address
             :others other-addresses})))
  ([my-address others]
     (join-cluster my-address others {}))
  ([my-address]
     (join-cluster my-address [] {})))

(defn get-members
  "Gets a list of all of the members in the cluster"
  [cluster]
  (:others cluster))

(defn get-my-address
  "Returns the id of the cluster member passed in"
  [cluster]
  (:me cluster))

(defn send-message
  "Sends a message to an address"
  [cluster address msg]
  (async/>!! (get (:channels cluster) address) msg)
  cluster)

(defn find-ping-target
  "Chooses a member to ping, returns the cluster and the target in a vector"
  [cluster]
  (let [members (get-members cluster)
        i (-> (get cluster :last-ping-target -1)
              inc
              (mod (count members)))
        members (if (= 0 i)
                  (loop [mbrs members]
                    (if (not= members mbrs)
                      mbrs
                      (recur (shuffle mbrs))))
                  members)
        target (get members i)
        cluster (assoc cluster
                  :last-ping-target i
                  :others members)]
    (vector cluster target)))

(defn find-k-number
  [members k-factor]
  (let [nr (count members)]
    (-> k-factor
        (* nr)
        round
        (max 1)
        (min nr))))

(defn find-k-ping-targets
  "Choose at most (min k (n - 1)) targes from the pool of members"
  ([cluster k]
     (let [n (min k (- (count (get-members cluster)) 1))]
       (loop [cluster cluster
              targets #{}]
         (if (= n (count targets)) [cluster (vec targets)]
             (let [[cluster target] (find-ping-target cluster)]
               (recur cluster
                      (conj targets target)))))))
  ([cluster]
     (find-k-ping-targets cluster (find-k-number (get-members cluster)
                                                 (:k-factor cluster)))))

(defn ping-member
  "Chooses a member from the cluster and sends a ping message"
  [cluster]
  (let [[cluster target] (find-ping-target cluster)]
    (-> cluster
        ((fn [c]
            (let [pinged (get (:pinged c) [])]
              (assoc c :pinged (conj pinged target)))))
        (send-message target {:type :ping}))))

(defmulti receive-message (fn [cluster msg]
                            (:type msg)))
(defmethod receive-message
  :timeout
  [cluster {:keys [target]}]
  (let [pinged (:pinged cluster)]
    (if (some #{target} pinged)
      (let [pinged (filter #{target} pinged)]
        )
      
      
      )
    )

  cluster)

(defn foo
  "I don't do a whole lot."
  [x]
  (println x "Hello, World!"))
