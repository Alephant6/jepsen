(ns jepsen.cockroach.timestamp-inversion
  "Reimplements the timestamp inversion workload so we can orchestrate three
  logical clients accessing two disjoint ranges. The goal is to show a reader
  that observes an old value from range A and a new value from range B within a
  single transaction."
  (:refer-clojure :exclude [test])
  (:require [clojure.java.jdbc :as j]
            [clojure.tools.logging :refer [info warn]]
            [jepsen [checker :as checker]
                    [client :as client]
                    [generator :as gen]]
            [jepsen.checker.timeline :as timeline]
            [jepsen.cockroach :as cockroach]
            [jepsen.cockroach.client :as c]
            [jepsen.cockroach.nemesis :as cln]
            [jepsen.reconnect :as rc]
            [knossos.op :as op]))

(def table-name :inversion_pairs)

(def default-iterations
  "How many orchestrated rounds each worker attempts by default."
  200)

(def default-await-timeout-ms
  "Maximum time (ms) to wait for inter-role coordination events."
  5000)

(def default-stagger
  "Average seconds between scenario attempts per worker thread."
  0.2)

(defn role-for-process
  "Maps a Jepsen process index to one of the three logical roles."
  [process]
  (if (integer? process)
    (case (mod (int process) 3)
      0 :reader
      1 :writer-a
      2 :writer-b)
    :reader))

(defn maybe-deliver!
  "Delivers v to promise p if it has not already been realized."
  [p v]
  (when (and p (not (realized? p)))
    (deliver p v)))

(defn await-signal
  "Waits for promise p with timeout-ms milliseconds, raising an ex-info on timeout."
  [p timeout-ms label]
  (let [timeout-val ::timeout
        v (deref p timeout-ms timeout-val)]
    (when (= timeout-val v)
      (throw (ex-info (str "Timed out waiting for " label)
                      {:jepsen/error :await-timeout
                       :await label})))
    v))

(defn now-nanos
  []
  (System/nanoTime))

(defn read-inversion-state
  "Reads the two rows manipulated by the workload."
  [conn]
  (->> (c/query conn [(format "select key, val, last_writer, iteration
                                from %s
                                order by key"
                              (name table-name))])
       (map #(select-keys % [:key :val :last_writer :iteration]))
       vec))

(defn new-iteration
  [id]
  {:id id
   :reader-a-read (promise)
   :write-a-done  (promise)
   :write-b-done  (promise)
   :completed     (atom 0)})

(defn ensure-iteration
  "Ensures coord holds an iteration map and returns it."
  [coord]
  (let [state (swap! coord
                     (fn [{:keys [current next-id] :as s}]
                       (if current
                         s
                         (let [id (or next-id 0)]
                           (-> s
                               (assoc :current (new-iteration id))
                               (assoc :next-id (inc id)))))))]
    (:current state)))

(defn complete-role!
  "Marks a role as complete and clears the iteration once all roles finish."
  [coord iter]
  (let [finished (swap! (:completed iter) inc)]
    (when (= finished 3)
      (swap! coord
             (fn [{:keys [current] :as s}]
               (if (and current (= (:id current) (:id iter)))
                 (assoc s :current nil)
                 s))))))

(defn fetch-key
  "Reads a single key from the inversion_pairs table."
  [tx key]
  (->> (c/query tx [(format "select val from %s where key = ?"
                            (name table-name))
                   key]
                {:row-fn :val})
       first))

(defn update-key!
  [tx key value iter role]
  (j/execute! tx [(format "update %s
                             set val = ?,
                                 last_writer = ?,
                                 iteration = ?
                             where key = ?"
                           (name table-name))
                  value (name role) iter key]))

(defn reset-keys!
  [conn iter-id]
  (c/with-txn-retry
    (c/with-txn [tx conn]
      (doseq [key ["A" "B"]]
        (j/execute! tx [(format "update %s
                                   set val = 0,
                                       last_writer = 'reset',
                                       iteration = ?
                                   where key = ?"
                                 (name table-name))
                        iter-id key])))))

(defn ensure-range-layout!
  "Splits the table at the A/B keys and relocates leases to early nodes."
  [test conn]
  (doseq [boundary ["A" "B"]]
    (try
      (c/split! conn table-name boundary)
      (catch Exception e
        (let [msg (.getMessage e)]
          (when-not (and msg (re-find #"already.*split" msg))
            (warn e "Failed to split range at" boundary))))))
  (let [range-a (c/show-range-for-row conn table-name "A")
        range-b (c/show-range-for-row conn table-name "B")
        store-map (c/store-ids-by-node conn)
        store-a  (get store-map 1)
        store-b  (get store-map 2)]
    (when-not range-a
      (warn "Unable to locate range metadata for key A"))
    (when-not range-b
      (warn "Unable to locate range metadata for key B"))
    (when (and (:range_id range-a) store-a)
      (try
        (c/relocate-lease! conn (:range_id range-a) store-a)
        (info "Relocated range for key A to store" store-a)
        (catch Exception e
          (warn e "Failed to relocate lease for key A"))))
    (when (and (:range_id range-b) store-b)
      (try
        (c/relocate-lease! conn (:range_id range-b) store-b)
        (info "Relocated range for key B to store" store-b)
        (catch Exception e
          (warn e "Failed to relocate lease for key B"))))))

(defn execute-reader
  [coord conn {:keys [await-ms]} iter]
  (let [await-ms (long await-ms)]
    (try
      (c/with-conn [c conn]
        (reset-keys! c (:id iter))
        (let [start-wall (now-nanos)
              result     (c/with-txn-retry
                           (c/with-txn [tx c]
                             (let [read-a (fetch-key tx "A")]
                               (maybe-deliver! (:reader-a-read iter)
                                               {:wall start-wall
                                                :value read-a})
                               (await-signal (:write-b-done iter) await-ms
                                             "writer-b completion")
                               (let [read-b (fetch-key tx "B")]
                                 {:role  :reader
                                  :reads {:a read-a
                                          :b read-b}}))))]
          result))
      (finally
        (complete-role! coord iter)))))

(defn execute-writer-a
  [coord conn {:keys [await-ms]} iter]
  (let [await-ms (long await-ms)]
    (try
      (await-signal (:reader-a-read iter) await-ms "reader first read (A)")
      (c/with-conn [c conn]
        (let [start-wall (now-nanos)
              result     (c/with-txn-retry
                           (c/with-txn [tx c]
                             (update-key! tx "A" 2 (:id iter) :writer-a)
                             {:role :writer-a
                              :write {:key "A"
                                      :value 2
                                      :wall {:start start-wall
                                             :end   (now-nanos)}}}))]
          (maybe-deliver! (:write-a-done iter) result)
          result))
      (finally
        (complete-role! coord iter)))))

(defn execute-writer-b
  [coord conn {:keys [await-ms]} iter]
  (let [await-ms (long await-ms)]
    (try
      (await-signal (:write-a-done iter) await-ms "writer-a completion")
      (c/with-conn [c conn]
        (let [start-wall (now-nanos)
              result     (c/with-txn-retry
                           (c/with-txn [tx c]
                             (update-key! tx "B" 2 (:id iter) :writer-b)
                             {:role :writer-b
                              :write {:key "B"
                                      :value 2
                                      :wall {:start start-wall
                                             :end   (now-nanos)}}}))]
          (maybe-deliver! (:write-b-done iter) result)
          result))
      (finally
        (complete-role! coord iter)))))

(defrecord TimestampInversionClient [coord table-created? conn]
  client/Client

  (open! [this _ node]
    (assoc this :conn (c/client node)))

  (setup! [this test]
    (locking table-created?
      (when (compare-and-set! table-created? false true)
        (c/with-conn [c conn]
          (j/execute! c [(format "create table if not exists %s
                                   (key string primary key,
                                    val int,
                                    last_writer string,
                                    iteration int)"
                                 (name table-name))])
          (doseq [key ["A" "B"]]
            (j/execute! c [(format "insert into %s
                                       (key, val, last_writer, iteration)
                                       values (?, 0, 'init', 0)
                                       on conflict (key)
                                       do update set val = excluded.val,
                                                       last_writer = excluded.last_writer,
                                                       iteration = excluded.iteration)"
                                     (name table-name))
                           key]))
          (ensure-range-layout! test c)))))

  (invoke! [this test op]
    (let [coord (:coord this)
          conn  (:conn this)
          opts  {:await-ms (long (:await-timeout-ms test default-await-timeout-ms))}]
      (c/with-exception->op op
        (try
          (case (:f op)
            :orchestrate
            (let [iter  (ensure-iteration coord)
                  role  (role-for-process (:process op))
                  value (case role
                          :reader   (execute-reader coord conn opts iter)
                          :writer-a (execute-writer-a coord conn opts iter)
                          :writer-b (execute-writer-b coord conn opts iter))]
              (assoc op :type :ok :value (assoc value :iteration (:id iter))))

            :final-read
            (c/with-conn [c conn]
              (assoc op :type :ok :value {:role :final
                                          :state (read-inversion-state c)}))

            (assoc op :type :fail :error [:unknown-op (:f op)]))
          (catch clojure.lang.ExceptionInfo e
            (if (= :await-timeout (:jepsen/error (ex-data e)))
              (assoc op :type :fail :error [:await-timeout (:await (ex-data e))])
              (throw e)))))))

  (teardown! [_ _]
    nil)

  (close! [_ _]
    (when conn
      (rc/close! conn))))

(defn inversion-case
  "Builds a summary for a single iteration's trio of roles."
  [entries]
  (let [by-role (into {} (map (juxt :role identity) entries))
        reader (:reader by-role)
        writer-a (:writer-a by-role)
        writer-b (:writer-b by-role)]
    (when (and reader writer-a writer-b)
      (let [a-read (get-in reader [:reads :a])
            b-read (get-in reader [:reads :b])
            a-write (get-in writer-a [:write :value])
            b-write (get-in writer-b [:write :value])
            anomaly? (not (and (= 0 a-read)
                               (= 2 b-read)))]
        {:iteration (:iteration reader)
         :reader reader
         :writer-a writer-a
         :writer-b writer-b
         :expected {:reader-a 0
                    :reader-b 2
                    :writer-a 2
                    :writer-b 2}
         :observed {:reader-a a-read
                    :reader-b b-read
                    :writer-a a-write
                    :writer-b b-write}
         :anomaly? anomaly?}))))

(defrecord TimestampInversionChecker []
  checker/Checker
  (check [_ _ _ history _]
    (let [vals (->> history
                    (filter op/ok?)
                    (keep (fn [op]
                            (when (= :orchestrate (:f op))
                              (:value op)))))
          cases (->> vals
                     (group-by :iteration)
                     (map (fn [[_ v]] (inversion-case v)))
                     (remove nil?))
          anomalies (filter :anomaly? cases)]
      {:valid? (empty? anomalies)
       :cases (count cases)
       :anomalies (map #(select-keys % [:iteration :expected :observed :reader :writer-a :writer-b])
                       anomalies)})))

(defn scenario-generator
  [iterations stagger]
  (->> (repeat {:f :orchestrate})
       (gen/limit (* iterations 3))
       (gen/stagger stagger)))

(defn test
  "Constructs the timestamp inversion Jepsen workload configuration."
  [opts]
  (let [iterations (get opts :iterations default-iterations)
        stagger    (get opts :stagger default-stagger)
        coord      (atom {:current nil :next-id 0})
        ;; Enforce the three-role structure regardless of CLI --concurrency.
        opts       (dissoc opts :concurrency)]
    (cockroach/basic-test
      (merge
        {:name   "timestamp-inversion"
         :client {:client (TimestampInversionClient. coord (atom false) nil)
                  :during (scenario-generator iterations stagger)
                  :final  (gen/once {:f :final-read})}
         :concurrency 3
         :model nil
         :checker (checker/compose
                    {:perf     (checker/perf)
                     :timeline (timeline/html)
                     :inversion (TimestampInversionChecker.)})
         :nemesis {:name "none"
                   :client (cln/none)}
         :time-limit (:time-limit opts 60)}
        opts))))
