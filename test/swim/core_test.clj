(ns swim.core-test
  (:require [clojure.test :refer :all]
            [swim.core :refer :all]))

(deftest basic-api-tests
  (testing "GIVEN a unique member id"
    (let [id "id"]
      (testing "WHEN I create a new swim network"
        (let [swim (create-cluster id)]
          (testing "THEN initially it contains only myself in the members list"
            (is [= 1 (count (get-members swim))])))))))
