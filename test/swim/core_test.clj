(ns swim.core-test
  (:require [clojure.test :refer :all]
            [swim.core :refer :all]))

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
