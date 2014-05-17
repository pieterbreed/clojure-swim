(ns swim.core-test
  (:require [clojure.test :refer :all]
            [swim.core :refer :all]
            [swim.channels-test :refer :all]))

(deftest round-tests
  (testing "GIVEN a list of floating point values"
    (let [l [-1.5 -1.4 -0.7 -0.2 0.2 0.5 1.0 1.1]]
      (testing "WHEN they are passed to the round function"
        (let [l (map round l)]
          (testing "THEN they should be rounded to the nearest, bigger integer"
            (is (= '(-2 -1 -1 0 0 1 1 1)
                   l))))))))

(deftest basic-api-tests
  (testing "GIVEN an address which represents myself"
    (let [address 1]
      (with-fake-message-sink
        (fn [create-fn sink]
          (testing "WHEN I join a swim network without knowledge of another member"
            (let [swim (join-cluster address [] {:create-channel-fn  create-fn})]
              
              (testing "THEN I should be able to find my own id"
                (is (= address (get-my-address swim))))
              (testing "THEN I create a new cluster with no members in the other-members list"
                (is [= 0 (count (get-members swim))]))))))

      (with-fake-message-sink
        (fn [create-fn sink]
          (testing "WHEN I join a swim network with knowledge of one other member"
            (let [other-address "2"
                  swim (join-cluster address [other-address] {:create-channel-fn create-fn})]
              
              (testing "THEN the cluster should know about one other member"
                (is (= 1 (count (get-members swim)))))
              (testing "THEN I should be able to send a message to the other member"
                (send-message swim (-> swim get-members first) "message")
                ))))))))

(deftest find-k-number-tests
  (testing "GIVEN one member and k-factor of 0.2"
    (let [members [:a]]
      (testing "WHEN the find-k-number is called"
        (let [k-number (find-k-number members 0.2)]
          (testing "THEN it should return 0 (at least one member if there is one)"
            (is (= 1 k-number)))))))
  (testing "GIVEN no members and k-factor of 0.5"
    (let [members []]
      (testing "WHEN the find-k-number is called"
        (let [k-number (find-k-number members 0.5)]
          (testing "THEN it should return 0"
            (is (= 0 k-number))))))))


(deftest find-ping-target-tests
  (testing "GIVEN a cluster with n members"
    (let [members [:a :b :c :d]
          cluster (join-cluster :me members)]

      (testing "WHEN I need to find a member to ping"
        (let [target (find-ping-target cluster)]

          (testing "THEN I should get the cluster and the target back"
            (let [[cluster target] target]
              (is (map? cluster))
              (is (every? #(some #{%} members) (get-members cluster)))))

          (testing "THEN the target should not be nil"
            (let [[cluster target] (find-ping-target cluster)]
              (is (not (nil? target)))))))

      (testing "WHEN I pick 2 * n targets to ping"
        (let [targets (loop [cluster cluster
                             targets '()
                             i (* 2 (count members))]
                        (if (= 0 i)
                          targets
                          (let [[cluster target] (find-ping-target cluster)]
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
          cluster (join-cluster :me members {:k-factor 0.5})]

      (testing "AND I'm choosing the default number of k-ping-targets"
        (let [[cluster targets] (find-k-ping-targets cluster)]

          (testing "THEN I should get half of the number of members back as targets"
            (is (/ (count members) 2)
                (count targets)))))

      (testing "AND some members have been chosen as ping targets before"
        (let [cluster (loop [cl cluster
                             i (/ (count members) 2)]
                        (if (= i 0) cl
                            (recur (get (find-ping-target cl) 0)
                                   (dec i))))]

          (testing "WHEN I'm picking multiple targets"
            
            (let [[cluster targets] (find-k-ping-targets
                                     cluster
                                     (* 2  (count members)))]

              (testing "THEN I should only be able to pick (n - 1) targets at maximum"
                (is (= (- (count members) 1)
                       (count targets))))

              (testing "THEN there should be no repititions"
                (is (= (count targets)
                       (count (set targets))))))))))))

(deftest basic-timestep-tests
  (testing "GIVEN a cluster with 2 members"
    (with-fake-message-sink
      (fn [create-fn sink]
        (let [cluster (join-cluster :me [:a :b :c] {:create-channel-fn create-fn})] 

          (testing "WHEN a ping-member is called"
            (let [cluster (ping-member cluster)]

              (testing "THEN the :pinged collection should not be empty"
                (is (not= 0 (count (get cluster :pinged)))))

              (testing "THEN the sink should contain a ping message for the pinged item"
                (let [target (first (:pinged cluster))
                      msgs (get @sink target)]
                  (is (= 1 (count msgs)))
                  (is (= {:type :ping} (first msgs))))))))))))

(deftest ack-timeout-message-tests
  (testing "GIVEN a cluster with 2 members"
    (with-fake-message-sink
      (fn [create-fn sink]
        (let [others [:a :b]
              me :me
              cluster (join-cluster me others {:create-channel-fn create-fn})]

          (testing "WHEN I send a ping message to one of the members"
            (let [cluster (ping-member cluster)
                  target (first (:pinged cluster))
                  other (->> cluster
                             get-members
                             (filter #(= % target))
                             first)]

              (testing "AND a timeout message is received"
                (let [cluster (receive-message cluster
                                               {:type :timeout
                                                :target target})]

                  (testing "THEN the other member should be sent a ping-req message"
                    (is (= {:type :ping-req
                            :target target}
                           (-> sink
                               (get other)
                               first)))))))))))))
