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
      (contains? target)))

(defn member-is-dead
  [cluster target]
  (-> cluster
      :failed
      (contains? target)))

(defn member-is-alive
  [cluster target]
  (and (not (member-is-dead cluster target))
       (not (member-is-suspected cluster target))))

(defn get-my-address
  "Returns the id of the cluster member passed in"
  [cluster]
  (:me cluster))

(defn get-members
  "Gets a list of all of the members in the cluster"
  [cluster]
  (-> cluster
      :others
      keys))

(defn -join-member-to-cluster*
  "Joins a member to a cluster and performs any init associated with that member"
  [cluster new-member-address]
  (let [cluster (update-in cluster [:others] conj {new-member-address {:incarnation-nr 0}})]
    [cluster '()]))

(defn -leave-member-from-cluster*
  [cluster target]
  [(-> cluster
       (update-in [:others] dissoc target))
   '()])

(defn join-cluster*
  "Creates a handle to a swim cluster using local id which must be unique to the cluster:
options:
{
:create-channel-fn function that will create a channel given an id
:k-factor value between 0 and 1 that will determine how many elements are chosen to send a ping-req to if a ping times out (default 0.67)
}
"
  ([my-address other-addresses options]
     (letfn [(join-message [cluster target]
               {:to target
                :msg {:type :alive
                      :incarnation-nr (:incarnation-nr cluster)
                      :target (get-my-address cluster)}})]
       (let [cluster (merge {:k-factor 0.67
                             :suspected #{}
                             :failed #{}
                             :incarnation-nr 0}
                            options 
                            {:me my-address
                             :others {}})
             [cluster msgs] (loop [cl cluster
                                   targets other-addresses
                                   msgs '()]
                              (if (nil? (first targets))
                                [cl msgs]
                                (let [[cl new-msgs] (-join-member-to-cluster* cl (first targets))]
                                  (recur cl
                                         (rest targets)
                                         (conj msgs (join-message cl (first targets)))))))]
         [cluster msgs])))
  ([my-address others]
     (join-cluster* my-address others {}))
  ([my-address]
     (join-cluster* my-address [] {})))


(defn find-ping-target*
  "Chooses a member to ping, returns the cluster and the target in a vector"
  ([cluster]
     (let [members (get-members cluster)
           members-ping-order (get cluster :member-ping-order (vec members))
           members-ping-order (->> members-ping-order
                                   (filter #(some #{%} members))
                                   vec)
           i (-> (get cluster :last-ping-target -1)
                 inc
                 (min (count members-ping-order))
                 (mod (count members-ping-order)))
           members-ping-order (if (= 0 i)
                                (loop [mbrs members-ping-order]
                                  (if (not= members-ping-order mbrs)
                                    mbrs
                                    (recur (shuffle mbrs))))
                                members-ping-order)
           target (get members-ping-order i)
           cluster (assoc cluster
                     :last-ping-target i
                     :member-ping-order members-ping-order)]
       (vector cluster '() target)))
  ([cluster but-not]
     (loop [[cluster msgs target] (find-ping-target* cluster)]
       (if (not= target but-not)
         [cluster msgs target]
         (recur (find-ping-target* cluster))))))

(defn find-k-number
  "Returns the number of members to ping, given the amount of members in the cluster and the k-factor"
  [nr-of-members k-factor]
  (-> k-factor
      (* nr-of-members)
      round
      (max 1)
      (min nr-of-members)))

(defn find-k-ping-targets*
  "Choose at most (min k (n - 1)) targes from the pool of members, but won't return but-not"
  ([cluster k but-not]
     (let [eligible-members (-> cluster
                                get-members
                                set
                                (disj but-not))
           n (min k
                  (count eligible-members))]
       (loop [cluster cluster
              msgs '()
              targets #{}]
         (if (= n (count targets)) [cluster msgs (vec targets)]
             (let [[cluster step-msgs target] (find-ping-target* cluster but-not)]
               (recur cluster
                      (concat msgs step-msgs)
                      (conj targets target)))))))
  ([cluster but-not]
     (find-k-ping-targets* cluster
                           (find-k-number (count (get-members cluster))
                                          (:k-factor cluster))
                           but-not)))

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
  (-join-member-to-cluster* cluster member-address))

(defmethod receive-message*
  :member-leaving
  [cluster {:keys [member-address]}]
  (-leave-member-from-cluster* cluster member-address))

(defn -receive-ping-timeout*
  [cluster target]
  (let [pinged (:pinged cluster)]
    (if (some #{target} pinged)
      (let [[cluster msgs targets] (find-k-ping-targets* cluster target)]
        (loop [cluster cluster
               ts targets
               msgs msgs]
          (if (= 0 (count ts))
            [cluster msgs]
            (let [ping-req-target (first ts)
                  other-targets (rest ts)]
              (recur (update-in-def cluster [:ping-reqed] #{}
                                    (fn [x]
                                      (conj x {:to ping-req-target
                                               :target target})))
                     other-targets
                     (concat msgs (list {:to ping-req-target
                                         :msg {:type :ping-req
                                               :target target}})))))))
      [cluster '()])))

(defn -receive-ping-req-timeout*
  [cluster target from]
  (let [pinged (:pinged cluster)]
    (if (and (some #{target} pinged)
             (some #{{:to from
                      :target target}}
                   (:ping-reqed cluster)))
      (let [cluster (update-in cluster [:ping-reqed] disj {:to from
                                                           :target target})]
        (if (->> cluster
                 :ping-reqed
                 (map :target)
                 (filter #(= % target))
                 count
                 (= 0))
          [(-> cluster
               (update-in [:suspected] conj target)
               (update-in [:pinged] disj target))
           '()]
          [cluster '()]))
      [cluster '()])))

(defmethod receive-message*
  :timeout
  [cluster {:keys [target timeout-type from]}]
  (condp = timeout-type
    :ping (-receive-ping-timeout* cluster target)
    :ping-req (-receive-ping-req-timeout* cluster target from)))

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
  (if (> incarnation-nr (get-incarnation-number-for cluster target))
    (let [cluster (-> cluster
                      (update-in-def [:failed] #{} disj target)
                      (update-in-def [:suspected] #{} disj target)
                      (update-in [:others] #(update-in % [target] assoc :incarnation-nr incarnation-nr)))]
      [cluster '()])
    [cluster '()]))

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

  (let [[cluster msgs] (if (= (get-my-address cluster) target)
                         (-receive-me-suspected cluster from)
                         [(update-in cluster [:suspected] conj target) '()])]
    [cluster msgs]))

(defmethod receive-message*
  :confirm
  [cluster {:keys [target incarnation-nr]}]
  [(-> cluster
       (update-in [:suspected] disj target)
       (update-in [:failed] conj target))
   '()])


(defn foo
  "I don't do a whole lot."
  [x]
  (println x "Hello, World!"))
