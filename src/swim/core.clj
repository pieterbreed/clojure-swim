(ns swim.core
  (:require [clojure.core.async :as async]))

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
  "Creates a handle to a swim cluster using local id which must be unique to the cluster"
  ([my-address other-addresses create-channel-fn]
     (-init-cluster 
      {:me my-address
       :others other-addresses
       :create-channel-fn create-channel-fn}))
  ([my-address others]
     (join-cluster my-address others create-channel))
  ([my-address]
     (join-cluster my-address [] create-channel)))

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
                  (shuffle members)
                  members)
        target (get members i)
        cluster (assoc cluster
                  :last-ping-target i
                  :others members)]
    (vector cluster target)))

(defn ping-member
  "Chooses a member from the cluster and sends a ping message via the channel ch"
  [cluster]
  (let [[cluster target] (find-ping-target cluster)]
    (-> cluster
        ((fn [c]
            (let [pinged (get (:pinged c) [])]
              (assoc c :pinged (conj pinged target)))))
        (send-message target {:type :ping}))))

(defn foo
  "I don't do a whole lot."
  [x]
  (println x "Hello, World!"))
