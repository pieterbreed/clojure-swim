(ns swim.channels-test
  (:require [clojure.test :refer :all]
            [clojure.core.async :as async]))

(defn create-fake-message-sink
  "Creates a way to send messages via channels that will all come into a common sink"
  []
  (let [sink (atom {})
        channels (atom [])
        stop-fn (fn []
                  (println (str "closing " (count @channels) " channels ..."))
                  (for [ch @channels]
                    (do (println (str "closing " ch))
                        (async/close! ch)))
                  (swap! channels (constantly [])))
        create-channel-fn (fn [id]
                            (let [ch (async/chan)
                                  _ (swap! channels conj ch)
                                  _ (async/go-loop [v (async/<! ch)]
                                                   (if-not (nil? v)
                                                     (do 
                                                       (swap! sink
                                                              (fn [s v] (assoc s id (conj (get s id []) v)))
                                                              v)
                                                       (recur (async/<! ch)))))]
                              ch))]
    [create-channel-fn sink stop-fn]))

(defn with-fake-message-sink
  "Creates a fake message sink, passes the create-fn and sink to a function and stops all of the channels afterward"
  [f]
  (let [[create-fn sink stop-fn] (create-fake-message-sink)]
    (f create-fn sink)
    (stop-fn)))

(deftest fake-message-sink-tests
  (testing "GIVEN a fake message sink create-channel implementation"
    (testing "WHEN I create a channel with a known id from it"
      (with-fake-message-sink
        (fn [create-channel sink]
          (let [ch (create-channel :id)]
            (testing "AND send a message through it"
              (do
                ;; waits for onto-chan to copy all of (range 10) into
                ;; the channel
                (async/<!! (async/onto-chan ch (range 10)))
                (testing "THEN I should see the message arrive in the sink, associated with the id"
                  (is (= 1 (count (keys @sink))))
                  (is (= 10 (count (get @sink :id))))
                  (is (= (vec (range 10))(get @sink :id))))))))))))
