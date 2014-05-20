(ns swim.utils)

(defn round
  "Naive implementation of rounding for floating point numbers. Returns an int. Rounds up, away from 0. Not bankers rounding"
  [fp]
  (let [sign (if (< 0 fp) 1 -1)]
    (-> fp
        (+ (* sign  0.5))
        int)))

