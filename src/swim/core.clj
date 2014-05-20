(ns swim.core
  (:require [clojure.core.async :as async]
            [swim.utils :refer :all]))

;; all API methods marked with '*' at the end of their name returns a vector
;; [new-cluster-state ((destination msg) <other results>)]

(defn -init-cluster
  [cluster]
  [cluster '()])

(defn join-cluster*
  "Creates a handle to a swim cluster using local id which must be unique to the cluster:
options:
{
:create-channel-fn function that will create a channel given an id
:k-factor value between 0 and 1 that will determine how many elements are chosen to send a ping-req to if a ping times out (default 0.67)
}
"
  ([my-address other-addresses options]
     (-init-cluster
      (merge {:k-factor 0.67}
             options 
            {:me my-address
             :others other-addresses})))
  ([my-address others]
     (join-cluster* my-address others {}))
  ([my-address]
     (join-cluster* my-address [] {})))

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

(defn find-ping-target*
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
    (vector cluster '() target)))

(defn find-k-number
  "Returns the number of members to ping, given the amount of members in the cluster and the k-factor"
  [nr-of-members k-factor]
  (-> k-factor
      (* nr-of-members)
      round
      (max 1)
      (min nr-of-members)))

(defn find-k-ping-targets*
  "Choose at most (min k (n - 1)) targes from the pool of members"
  ([cluster k]
     (let [n (min k (- (count (get-members cluster)) 1))]
       (loop [cluster cluster
              targets #{}]
         (if (= n (count targets)) [cluster '() (vec targets)]
             (let [[cluster target] (find-ping-target* cluster)]
               (recur cluster
                      (conj targets target)))))))
  ([cluster]
     (find-k-ping-targets* cluster (find-k-number (count (get-members cluster))
                                                  (:k-factor cluster)))))

(defn ping-member
  "Chooses a member from the cluster and sends a ping message"
  [cluster]
  (let [[cluster target] (find-ping-target* cluster)]
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
      (let [[cluster targets] (find-k-ping-targets* cluster)]
        (loop [ts targets
               cl cluster]
          (if (= 0 (count ts))
            cl
            (let [cl (send-message cl
                            (first ts)
                            {:type :ping-req
                             :target target})]
              (recur (rest ts)
                     cl)))))
      cluster)))

(defn foo
  "I don't do a whole lot."
  [x]
  (println x "Hello, World!"))
