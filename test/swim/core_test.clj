(ns swim.core-test
  (:require [clojure.test :refer :all]
            [swim.core :refer :all]
            [swim.channels-test :refer :all]))

(deftest basic-api-tests
  (testing "GIVEN an address which represents myself"
    (let [address 1]
      
      (testing "WHEN I join a swim network without knowledge of another member"
        (let [swim (join-cluster address)]
          
          (testing "THEN I should be able to find my own id"
            (is (= address (get-my-address swim))))
          (testing "THEN I create a new cluster with no members in the other-members list"
            (is [= 0 (count (get-members swim))]))))
      
      (testing "WHEN I join a swim network with knowledge of one other member"
        (let [other-address "2"
              swim (join-cluster address [other-address])]
          
          (testing "THEN the cluster should know about one other member"
            (is (= 1 (count (get-members swim)))))
          (testing "THEN I should be able to send a message to the other member"
            (send-message (-> swim get-members first) "message")
            ))))))

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


