(ns jepsen.cockroach.minimal
  "Minimal sequential read/write workload for smoke testing CockroachDB."
  (:refer-clojure :exclude [test])
  (:require [jepsen.checker :as checker]
            [jepsen.cockroach :as cockroach]
            [jepsen.cockroach.sequential :as sequential]
            [jepsen.generator :as g]))

(defn test
  "Runs a pared-down variant of the sequential test with a single table and a
  small client pool. This exercises the sequential client with basic write and
  read operations, which is useful for quick sanity checks."
  [opts]
  (let [workload (sequential/gen 1)
        keyrange (atom {})]
    (cockroach/basic-test
      (merge {:name "minimal"
              :key-count 1
              :keyrange keyrange
              :client {:client (sequential/->Client 1 (atom false) nil)
                       :during (g/stagger 1/50 workload)
                       :final nil}
              :checker (checker/compose
                         {:perf (checker/perf)
                          :sequential (sequential/checker)})}
             opts))))
