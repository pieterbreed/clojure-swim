(ns swim.core-test
  (:require [clojure.test :refer :all]
            [swim.core :refer :all]
            [swim.channels-test :refer :all]))

(deftest basic-api-tests
  (testing "GIVEN an address which represents myself"
    (let [address 1]
      (with-fake-message-sink
        (fn [create-fn sink]
          (testing "WHEN I join a swim network without knowledge of another member"
            (let [swim (join-cluster address [] create-fn)]
              
              (testing "THEN I should be able to find my own id"
                (is (= address (get-my-address swim))))
              (testing "THEN I create a new cluster with no members in the other-members list"
                (is [= 0 (count (get-members swim))]))))))

      (with-fake-message-sink
        (fn [create-fn sink]
          (testing "WHEN I join a swim network with knowledge of one other member"
            (let [other-address "2"
                  swim (join-cluster address [other-address] create-fn)]
              
              (testing "THEN the cluster should know about one other member"
                (is (= 1 (count (get-members swim)))))
              (testing "THEN I should be able to send a message to the other member"
                (send-message swim (-> swim get-members first) "message")
                ))))))))

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

          (testing "THEN the sequence in which targets are chosen are random"
            (is (apply not= (partition (count members) targets))))

          (testing "THEN every target should be picked exactly twice"
            (is (every? #(= 2 %) (vals (frequencies targets))))))))))

(deftest basic-timestep-tests
  (testing "GIVEN a cluster with 2 members"
    (with-fake-message-sink
      (fn [create-fn sink]
        (let [cluster (join-cluster :me [:a :b :c] create-fn)] 
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
              cluster (join-cluster me others create-fn)]
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
                    (= {:type :ping-req
                        :target target}
                       (-> sink
                           (get other)
                           first))))))))))))
