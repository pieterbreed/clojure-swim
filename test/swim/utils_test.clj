(ns swim.utils-test
  (:require [clojure.test :refer :all]
            [swim.utils :refer :all]))

(deftest round-tests
  (testing "GIVEN a list of floating point values"
    (let [l [-1.5 -1.4 -0.7 -0.2 0.2 0.5 1.0 1.1]]
      (testing "WHEN they are passed to the round function"
        (let [l (map round l)]
          (testing "THEN they should be rounded to the nearest, bigger integer"
            (is (= '(-2 -1 -1 0 0 1 1 1)
                   l))))))))

(deftest update-in-with-default-tests
  (testing "GIVEN a nested data structure"
    (let [data {:a {:b {:c "value"}}}]
      (testing "WHEN update-in-def is called with path [:a :b :c] and a function returning \"changed\""
        (let [data (update-in-def data [:a :b :c] "default" (constantly "changed"))]
          (testing "THEN (get-in [:a :b :c]  should be \"changed"
            (is (= "changed" (get-in data [:a :b :c]))))))

      (testing "WHEN update-in-def is called with path [:a :b :d] and an identify function"
        (let [data (update-in-def data [:a :b :d] "default" identity)]
          (testing "THEN (get-in [:a :b :d] should be \"default\""
            (is (= "default" (get-in data [:a :b :d])))))))))



