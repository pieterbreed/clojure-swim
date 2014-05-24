(ns swim.core
  (:require [clojure.core.async :as async]
            [swim.utils :refer :all]))

;; all API methods marked with '*' at the end of their name returns a vector
;; [new-cluster-state ((destination msg) <other results>)]

(defn get-suspicious-members
  "Gets all of the members of the cluster that is currently suspected"
  [cluster]
  (:suspected cluster))

(defn get-incarnation-number
  "Retrieves the this node's incarnation number"
  [cluster]
  (:incarnation-nr cluster))

(defn get-incarnation-number-for
  "Retrieves the local incarnation number for a member"
  [cluster target]
  (-> cluster
      :others
      (get target)
      :incarnation-nr
      (or 0)))

(defn member-is-suspected
  [cluster target]
  (-> cluster
      :suspected
      (contains? target)
      not))

(defn member-is-dead
  [cluster target]
  (-> cluster
      :failed
      (contains? target)))

(defn member-is-alive
  [cluster target]
  (and (not (member-is-dead cluster target))
       (not (member-is-suspected cluster target))))

(defn join-cluster*
  "Creates a handle to a swim cluster using local id which must be unique to the cluster:
options:
{
:create-channel-fn function that will create a channel given an id
:k-factor value between 0 and 1 that will determine how many elements are chosen to send a ping-req to if a ping times out (default 0.67)
}
"
  ([my-address other-addresses options]
     (merge {:k-factor 0.67
             :suspected #{}
             :failed #{}}
            options 
            {:me my-address
             :others (->> other-addresses
                          (map #(hash-map % {}))
                          (apply merge))}))
  ([my-address others]
     (join-cluster* my-address others {}))
  ([my-address]
     (join-cluster* my-address [] {})))

(defn get-members
  "Gets a list of all of the members in the cluster"
  [cluster]
  (-> cluster
      :others
      keys))

(defn get-my-address
  "Returns the id of the cluster member passed in"
  [cluster]
  (:me cluster))

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
              msgs '()
              targets #{}]
         (if (= n (count targets)) [cluster msgs (vec targets)]
             (let [[cluster step-msgs target] (find-ping-target* cluster)]
               (recur cluster
                      (concat msgs step-msgs)
                      (conj targets target)))))))
  ([cluster]
     (find-k-ping-targets* cluster (find-k-number (count (get-members cluster))
                                                  (:k-factor cluster)))))

(defn ping-target*
  "Pings a specific member"
  [cluster msgs target]
  (let [cluster (update-in-def cluster [:pinged] #{}
                               #(conj % target))
        msgs (concat msgs (list {:to target
                                 :msg {:type :ping}}))]
    (vector cluster msgs target)))

(defn ping-member*
  "Chooses a member from the cluster and sends a ping message"
  [cluster]
  (let [[cluster msgs target] (find-ping-target* cluster)]
    (ping-target* cluster msgs target)))

(defmulti receive-message* (fn [cluster msg]
                             (:type msg)))

(defmethod receive-message*
  :member-joining
  [cluster {:keys [member-address]}]
  [cluster '()])

(defmethod receive-message*
  :member-leaving
  [cluster {:keys [member-address]}]
  [cluster '()])

(defmethod receive-message*
  :timeout
  [cluster {:keys [target]}]
  (let [pinged (:pinged cluster)]
    (if (some #{target} pinged)
      (let [[cluster msgs targets] (find-k-ping-targets* cluster)]
        (loop [ts targets
               msgs msgs]
          (if (= 0 (count ts))
            [cluster msgs]
            (recur (rest ts)
                   (concat msgs (list {:to (first ts)
                                       :msg {:type :ping-req
                                             :target target}}))))))
      [cluster '()])))

(defmethod receive-message*
  :ack
  [cluster {:keys [from for-target]}]
  (let [me (get-my-address cluster)
        [cluster msgs] (loop [needs-responses (->> (:ping-req cluster)
                                                   (filter #(= (:target %) for-target))
                                                   (map :from))
                              cluster (update-in-def cluster [:pinged] #{}
                                                     disj for-target)
                              msgs '()]
                         (if (= 0 (count needs-responses))
                           [cluster msgs]
                           (let [x (first needs-responses)
                                 msg {:to x
                                      :msg {:type :ack
                                            :from me
                                            :for-target for-target}}]
                             (recur (rest needs-responses)
                                    (update-in cluster [:ping-req] disj x)
                                    (conj msgs msg)))))]
    (if (not= from for-target)
      [cluster msgs]
      [cluster (conj msgs {:to :disseminate
                           :msg {:type :alive
                                 :incarnation-nr (get-incarnation-number-for cluster for-target)
                                 :target for-target}})])))

(defmethod receive-message*
  :ping-req
  [cluster {:keys [from for-target]}]
  (let [cluster (update-in-def cluster [:ping-req] #{}
                               conj {:from from
                                     :target for-target})]
    (ping-target* cluster '() for-target)))

(defmethod receive-message*
  :alive
  [cluster {:keys [target incarnation-nr]}]
  [cluster '()])

(defn -receive-me-suspected
  [cluster from]
  (let [cluster (update-in cluster [:incarnation-nr] inc)]
    [cluster (list {:to from
                    :msg {:type :alive
                          :target (get-my-address cluster)
                          :incarnation-nr (get-incarnation-number cluster)}})]))

(defmethod receive-message*
  :suspected
  [cluster {:keys [target incarnation-nr from]}]
  (if (= (get-my-address cluster)
         target)
    (-receive-me-suspected cluster from)
    [cluster '()]))

(defmethod receive-message*
  :confirm
  [cluster {:keys [target incarnation-nr]}]
  [cluster '()])


(defn foo
  "I don't do a whole lot."
  [x]
  (println x "Hello, World!"))
