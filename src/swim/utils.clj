(ns swim.utils)

(defn round
  "Naive implementation of rounding for floating point numbers. Returns an int. Rounds up, away from 0. Not bankers rounding"
  [fp]
  (let [sign (if (< 0 fp) 1 -1)]
    (-> fp
        (+ (* sign  0.5))
        int)))

(defn update-in-def
  "Updates the map m's value at path using a function to modify the value. Similar to update-in, but suplies default value instead of nil if the path corresponds to nil"
  [m p def f & args]
  (update-in m p
             (fn [v]
               (apply f 
                      (if (nil? v) def v)
                      args))))
