(ns swim.core-test
  (:require [clojure.test :refer :all]
            [swim.core :refer :all]
            
            [swim.channels-test :refer :all]))

(defn debug [x]
  (print "DEBUG: ")
  (clojure.pprint/pprint x)
  x)

(defn cluster?
  "Tests whether a value represents a valid cluster"
  [v]
  (and (map? v)
       (contains? v :others)
       (contains? v :me)))

(deftest basic-api-tests
  (testing "GIVEN an address which represents myself"
    (let [address :me]

      (testing "WHEN I join a swim network without other members"
        (let [[cluster & _] (join-cluster* address [])]
          
          (testing "THEN I should be able to find my own id"
            (is (= address (get-my-address cluster))))
          
          (testing "THEN I create a new cluster with no members in the other-members list"
            (is [= 0 (count (get-members cluster))]))))

      (testing "WHEN I join a swim network with knowledge of one other member"
        (let [other-address :a
              [cluster & _] (join-cluster* address [other-address])]
          
          (testing "THEN the cluster should know about one other member"
            (is (= 1 (count (get-members cluster))))))))))


(deftest find-k-number-tests
  (testing "GIVEN one member and k-factor of 0.2"
    (let [members [:a]
          nr (count members)]

      (testing "WHEN the find-k-number is called"
        (let [k-number (find-k-number nr 0.2)]

          (testing "THEN it should return 0 (at least one member if there is one)"
            (is (= 1 k-number)))))))

  (testing "GIVEN no members and k-factor of 0.5"
    (let [members []
          nr (count members)]

      (testing "WHEN the find-k-number is called"
        (let [k-number (find-k-number nr 0.5)]

          (testing "THEN it should return 0"
            (is (= 0 k-number))))))))

(deftest find-ping-target-tests
  (testing "GIVEN a cluster with n members"
    (let [members [:a :b :c :d]
          [cluster & _] (join-cluster* :me members)]

      (testing "WHEN I need to find a member to ping"
        (let [[cluster _ target] (find-ping-target* cluster)]

          (testing "THEN I should get the cluster and the target back"
            (is (cluster? cluster)))

          (testing "THEN the target should be in the members list"
            (some #{target} (get-members cluster)))

          (testing "THEN the target should not be nil"
            (is (not (nil? target))))))

      (testing "WHEN I pick 2 * n targets to ping"
        (let [targets (loop [cluster cluster
                             targets '()
                             i (* 2 (count members))]
                        (if (= 0 i)
                          targets
                          (let [[cluster _ target] (find-ping-target* cluster)]
                            (recur cluster
                                   (conj targets target)
                                   (dec i)))))]

          (testing "THEN the sequence in which targets are chosen are changing"
            (is (apply not= (partition (count members) targets))))

          (testing "THEN every target should be picked exactly twice"
            (is (every? #(= 2 %) (vals (frequencies targets))))))))))


(deftest pick-k-ping-targets-tests
  (testing "GIVEN a cluster with many known members and default k factor of 0.5"
    (let [members [:a :b :c :d :e :f]
          [cluster & _] (join-cluster* :me members {:k-factor 0.5})]

      (testing "AND I'm choosing the default number of k-ping-targets"
        (let [[cluster _ targets] (find-k-ping-targets* cluster)]

          (testing "THEN I should get half of the number of members back as targets"
            (is (/ (count members) 2)
                (count targets)))))

      (testing "AND some members have been chosen as ping targets before"
        (let [cluster (loop [cl cluster
                             i (/ (count members) 2)]
                        (if (= i 0) cl
                            (recur (get (find-ping-target* cl) 0)
                                   (dec i))))]

          (testing "WHEN I'm picking multiple targets"
            
            (let [[cluster _ targets] (find-k-ping-targets*
                                       cluster
                                       (* 2  (count members)))]

              (testing "THEN I should only be able to pick (n - 1) targets at maximum"
                (is (= (- (count members) 1)
                       (count targets))))

              (testing "THEN there should be no repititions"
                (is (= (count targets)
                       (count (set targets))))))))))))




(deftest ping-tests
  (testing "GIVEN a cluster with 3 members"
    (let [members [:a :b :c]
          [cluster msgs] (join-cluster* :me members)]

      (testing "WHEN a ping-member is called"
        (let [[cluster msgs target] (ping-member* cluster)]

          (testing "THEN the :pinged collection should not be empty"
            (is (= 1 (count (get cluster :pinged))))
            (is (some #{target} members)))
          

          (testing "THEN the output messages should contain a ping message for the pinged item"
            (let [target (first (:pinged cluster))]
              (is (= 1 (count msgs)))
              (is (= {:to target
                      :msg {:type :ping}}
                     (first msgs)))))

          (testing "AND an ack message is received from the pinged member"
            (let [[cluster & _] (receive-message* cluster {:type :ack
                                                           :from target
                                                           :for-target target})]
              (testing "THEN the state should change to reflect that we are not waiting for an ack anymore"
                (is (= 0 (count (get cluster :pinged)))))))))


      (testing "WHEN a ping-req message is received from :a for :b"
        (let [[cluster msgs] (receive-message* cluster {:type :ping-req
                                                        :for-target :b
                                                        :from :a})]
          (testing "THEN a ping should have been sent to :b"
            (is (= {:to :b
                    :msg {:type :ping}}
                   (first msgs))))

          (testing "WHEN an ack is received from :b"
            (let [[cluster msgs] (receive-message* cluster {:type :ack
                                                            :from :b
                                                            :for-target :b})]

              (testing "THEN it should be forwarded back to :a"
                (is (= {:to :a
                        :msg {:type :ack
                              :from (get-my-address cluster)
                              :for-target :b}}
                       (first msgs)))))))))))


(deftest ack-timeout-message-tests
  (testing "GIVEN a cluster with 3 members and a k-factor of 1.0"
    (let [others [:a :b :c]
          me :me
          [cluster msgs] (join-cluster* me others {:k-factor 1.0})]
      (testing "WHEN I send a ping message to one of the members"
        (let [[cluster msgs] (ping-member* cluster)
              target (first (:pinged cluster))
              others (-> cluster
                         get-members
                         set
                         (disj target))]
          (testing "AND a ping timeout message is received"
            (let [[cluster msgs] (receive-message* cluster
                                                   {:type :timeout
                                                    :timeout-type :ping
                                                    :target target})
                  
                  msgs (apply hash-set msgs)]
              (testing "THEN both other members should be sent a ping-req message"
                (is (contains? msgs
                               {:to (first others)
                                :msg {:type :ping-req
                                      :target target}}))
                (is  (contains? msgs
                                {:to (second others)
                                 :msg {:type :ping-req
                                       :target target}})))
              (testing "AND an ack is received from one of the others"
                (let [first-other (first others)
                      other-other (first (disj others first-other))
                      [cluster & _] (receive-message* cluster
                                                      {:type :ack
                                                       :from first-other
                                                       :for-target target})]
                  (testing "THEN when the other other members sends an ack it should be a no-op"
                    (let [[cluster-after-other-other & _] (receive-message* cluster
                                                                      {:type :ack
                                                                       :from other-other
                                                                       :for-target target})]
                      (is (= cluster cluster-after-other-other)))))))))))))

(deftest ack-timeout-and-ping-req-timeouts-tests
  (testing "GIVEN a cluster with 3 members and a k-factor of 2/3"
    (let [members [:a :b :c]
          me :me
          [cluster msgs] (join-cluster* me members
                                        {:k-factor 0.67})]
      (testing "WHEN one of the cluster members is pinged"
        (let [[cluster msgs target] (ping-member* cluster)]
          (testing "AND a timeout is received for the target's ping"
            (let [[cluster new-msgs] (receive-message* cluster
                                                       {:type :timeout
                                                        :timeout-type :ping
                                                        :target target})
                  msgs (concat msgs new-msgs)]
              (testing "THEN two members should have been sent ping-reqs"
                (let [ping-req-targets (->> new-msgs
                                            (filter #(= :ping-req (-> % :msg :type)))
                                            (map :to)
                                            (apply list))]
                  (is (= 2 (count ping-req-targets)))
                  (testing "WHEN timeouts are received for both ping-reqs"
                    (let [[cluster msgs & _] (receive-message* cluster
                                                               {:type :timeout
                                                                :timeout-type :ping-req
                                                                :from (first ping-req-targets)
                                                                :target target})
                          [cluster new-msgs & _] (receive-message* cluster
                                                                   {:type :timeout
                                                                    :timeout-type :ping-req
                                                                    :from (second ping-req-targets)
                                                                    :target target})
                          msgs (concat msgs new-msgs)]
                      (testing "THEN the target should be marked as suspicious"
                        (is (some #{target} (get-suspicious-members cluster))))
                      (testing "THEN a messages should have been sent out to the cluster that that member has been marked as suspicious"
                        (some #(= % {:type :suspicious
                                     :target target})
                              (map :msg new-msgs))))))))))))))
