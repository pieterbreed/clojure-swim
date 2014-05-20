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



