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

(defn foo
  "I don't do a whole lot."
  [x]
  (println x "Hello, World!"))
