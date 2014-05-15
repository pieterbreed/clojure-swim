(ns swim.core
  (:require [clojure.core.async :as async]))

(defn join-cluster
  "Creates a handle to a swim cluster using local id which must be unique to the cluster"
  ([my-address]
     {:me my-address
      :others []})
  ([my-address other-addresses]
     {:me my-address
      :others other-addresses}))

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
  [address msg]
  nil)

(defn find-ping-target
  "Chooses a member to ping, returns the cluster and the target in a vector"
  [cluster]
  (let [members (get-members cluster)
        i (-> (get cluster :last-ping-target -1)
              inc
              (mod (count members)))
        target (get members i)
        cluster (assoc cluster :last-ping-target i)]
    (vector cluster target)))

(defn ping-member
  "Chooses a member from the cluster and sends a ping message via the channel ch"
  [cluster ch]
  (let [[cluster target] (find-ping-target cluster)
        pinged (-> cluster
                   (get :pinged [])
                   (conj target))]
    (assoc assoc cluster :pinged pinged)))

(defn foo
  "I don't do a whole lot."
  [x]
  (println x "Hello, World!"))
