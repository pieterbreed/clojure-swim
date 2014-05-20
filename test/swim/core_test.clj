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
  (testing "GIVEN a cluster with 2 members"
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
                (is (= 0 (count (get cluster :pinged)))))))

          (testing "AND an ack message is received from the pinged member"
            (let [[cluster & _] (receive-message* cluster {:type :ack
                                                           :from (->> (get-members cluster)
                                                                      (filter #(not= % target))
                                                                      first)
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
                   (first msgs)))))))))


(deftest ack-timeout-message-tests
  (testing "GIVEN a cluster with 2 members"
    (let [others [:a :b]
          me :me
          [cluster msgs] (join-cluster* me others)]
      (testing "WHEN I send a ping message to one of the members"
        (let [[cluster msgs] (ping-member* cluster)
              target (first (:pinged cluster))
              other (->> cluster
                         get-members
                         (filter #(not= % target))
                         first)]
          (testing "AND a timeout message is received"
            (let [[cluster msgs] (receive-message* cluster
                                                   {:type :timeout
                                                    :target target})]
              (testing "THEN the other member should be sent a ping-req message"
                (is (= {:to other
                        :msg {:type :ping-req
                              :target target}}
                       (first msgs)))))))))))
