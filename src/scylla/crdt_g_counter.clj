(ns scylla.crdt-g-counter
  "== OVERVIEW ==
   
   The test is inspired by the idea of Operation-based CRDT / CmRDT Counter.
   Not sure if it is academically accurate to tag this approach as CmRDT, 
   nevertheless, it allows us to build a sophisticated enough workload for more detailed batch testing 
   including atomicity checks, isolation verification, and row deletion/update operations.
   
   == MODEL ==
   
   The counter is modeled as a group of rows within the same partition defined by the ID partitioning key.
   
   The rows are differentiated by the clustering column OPERATION_ID. There are two types of rows:
   - The Operation Row: 
     Represents a single counter modification operation. 
     Each operation has its own unique OPERATION_ID.
   - The Summary Row: 
     The only one row per counter (ID).
     Serves the purpose of preventing the infinite growth of the operation rows.
     Eventually, the value of each operation row will be aggregated in the Summary Row and the operation row will be deleted.
     Has a constant OPERATION_ID = 'SUMM'.
   
   Both Summary and Operation rows keep their values in the VALUE column.
   The total value of the counter is the sum of VALUE columns of the summary row and all not deleted/aggregated rows.
   
   
   Such a model allows us to modify the counter concurrently by multiple workers 
   without any distributed locks or consensus protocols involved.
   But workers must follow the rules:
   - Only one worker at a time can be responsible for garbage collection and aggregation.
   - OPERATION_ID values for operation rows must be unique within the same counter (ID).
   - Aggregating operations must be executed in a single-partitioned batch to ensure isolation and atomicity.
   
   == EXAMPLE ==
   
   1. Initial state for the counter 0 with total value = 0:

   [{:id 0, :operation_id: 'SUMM', :value: 0}]
     
   2. Worker X increments the counter 0 with 1, so the total value = 0:
   
   [{:id 0, :operation_id: 'SUMM', :value: 0}
   {:id 0, :operation_id: 'x-0', :value: 1}]
   
   3. Worker Y increments the counter 0 with 1, so the total value = 2:
   
   [{:id 0, :operation_id: 'SUMM', :value: 0}
   {:id 0, :operation_id: 'x-0', :value: 1}
   {:id 0, :operation_id: 'y-0', :value: 1}]
   
   4. Worker N is responsible for garbage collection,
      increments the counter 0 with and aggregates all the previous operations, 
      so the total value = 3
   
   [{:id 0, :operation_id: 'SUMM', :value: 3}]
   
   == TEST DETAILS ==
   
   Using this model we can leverage the already existing workload and checker for cql counters. 
   So this test works only with the single counter having ID = 0 
   and allowed operations issued by workers are either counter read or counter increment by 1.
   
   The worker responsible for aggregation and garbage collection 
   is chosen dynamically - the first worker issuing an increment operation 
   wins atomically assigning (via atom) its :client-id as :gc-operator-client-id
   
   The :crdt-g-counter table includes two optional columns to tune workload:
   - :deleted column: 
     Using --homebrewed-tombstones argument we can replace delete operations with updates.
     In that case, we update rows with :deleted = true instead of deletion, 
     and such rows are ignored during the total counter's value calculation. 
   - :extra_payload:
     With --extra-payload-size argument it is possible to add some additional payload 
     to each row by setting a byte array of the configured size to the :extra_payload column.
     Could be useful to tune the duration of the partition read/write operations."
  (:require [clojure.tools.logging :refer [trace]]
            [jepsen
             [client    :as client]
             [checker   :as checker]
             [generator :as gen]]
            [qbits.alia :as alia]
            [qbits.alia.policy.retry :as retry]
            [qbits.hayt :as cql]
            [scylla [client :as c]]))

(def counter-id 0)
(def summ-operation-id "SUMM")

(defn generate-payload
  "Returns byte array of the given size"
  [size]
  (if (and (some? size) (pos? size)) 
    (byte-array (shuffle (range size))) 
    nil))

(defn set-if-absent 
  "Sets a new value to an atom if it contains nil and returns this value. 
   If the atom already has not-nil value, the current atom's value is returned" 
  [atom proposed-value]
  (if (compare-and-set! atom nil proposed-value)
    proposed-value
    @atom))

(defn gc-operator?
  "True if the client is responsible for GC operations"
  [client-id gc-operator]
  (= client-id (set-if-absent gc-operator client-id)))

(defn execute-write
  "Execute write CQL statement"
  [session test statement]
  (let [params (merge {:consistency :quorum
                       :retry-policy  (retry/fallthrough-retry-policy)}
                      (c/write-opts test))]
    (alia/execute session statement params)))

(defn execute-read
  "Execute read CQL statement"
  [session test statement]
  (let [params (merge {:consistency :quorum
                       :retry-policy (retry/fallthrough-retry-policy)}
                      (c/read-opts test))]
    (alia/execute session statement params)))

(defn increment-counter 
  "Increment counter by inserting an operation row for the given client-id"
  [session operation-id value test]
  (let [insert-increment (cql/insert :crdt_g_counters
                                     (cql/values [[:id counter-id]
                                                  [:operation_id operation-id]
                                                  [:value value]
                                                  [:extra_payload (generate-payload (:extra-payload-size test))]
                                                  [:deleted false]]))]
    (trace "INCREMENT COUNTER-ID=" counter-id " BY VALUE=" value ": " insert-increment)
    (execute-write session 
                   test 
                   insert-increment)))

(defn get-delete-statement 
  "Returns either actual delete statement or update statement with deleted flag in true"
  [record test]
  (if (:homebrewed-tombstones test) 
    (cql/insert :crdt_g_counters 
                (cql/values [[:id counter-id] 
                             [:operation_id (:operation_id record)]
                             [:deleted true]]))
    (cql/delete :crdt_g_counters 
                (cql/where [[= :id counter-id] 
                            [= :operation_id (:operation_id record)]]))))

(defn increment-counter-with-gc
  "Increment counter directly incrementing SUMM row and merging all other operations into it"
  [session value test]
  (let [all-data (execute-read
                  session
                  test
                  (cql/select :crdt_g_counters
                              (cql/where [[= :id counter-id]])))
        new-value (->> all-data
                       (filter #(= (:deleted %1) false))
                       (map :value)
                       (reduce + value))
        delete-statements (->> all-data
                               (filter #(= (:deleted %1) false))
                               (filter #(not= (:operation_id %1) summ-operation-id))
                               (map #(get-delete-statement %1 test)))
        update-summ-statement (cql/insert :crdt_g_counters
                                          (cql/values [[:id counter-id]
                                                       [:operation_id summ-operation-id]
                                                       [:value new-value]
                                                       [:extra_payload (generate-payload (:extra-payload-size test))]
                                                       [:deleted false]]))
        batch-increment-with-gc (cql/batch 
                                 (apply cql/queries 
                                        (conj delete-statements 
                                              update-summ-statement)))]
    (trace "INCREMENT COUNTER-ID=" counter-id " BY VALUE=" value " WITH GC: " batch-increment-with-gc)
    (execute-write session 
                   test 
                   batch-increment-with-gc)))

(defn read-counter-value 
  "Read total value for the given counter-id 
   considering both aggregated value from the SUMM row and separated incremental operations" 
  [session counter-id test]
  (let [all-data (execute-read session
                               test
                               (cql/select :crdt_g_counters 
                                           (cql/where [[= :id counter-id]])))
        counter-value (->> all-data
                           (filter #(= (:deleted %1) false))
                           (map :value)
                           (reduce + 0))]
    (trace "READ COUNTER-ID=" counter-id " WITH VALUE=" counter-value ": " all-data)
    counter-value))

(defn next-operation-id 
  "Generates next operation id using client id and client-local counter" 
  [client]
  (str 
   (:client-id client) 
   "-" 
   (swap! (:op-id-counter client) inc)))

(defrecord CrdtCounterClient [tbl-created? conn]
  client/Client

  (open! [this test node]
    (assoc this 
           :conn (c/open test node) 
           :client-id (swap! (:new-client-id-ctr test) inc)
           :op-id-counter (atom 0)))

  (setup! [_ test]
    (let [s (:session conn)]
      (locking tbl-created?
        (when (compare-and-set! tbl-created? false true)
          (c/retry-each
            (alia/execute s (cql/create-keyspace
                              :jepsen_keyspace
                              (cql/if-exists false)
                              (cql/with {:replication {:class :SimpleStrategy 
                                                       :replication_factor 3}})))
            (alia/execute s (cql/use-keyspace :jepsen_keyspace))
            (alia/execute s (cql/create-table
                              :crdt_g_counters
                              (cql/if-exists false)
                              (cql/column-definitions {:id            :int
                                                       ; counter id 
                                                       :operation_id  :text
                                                       ; operation identifier: each counter has 
                                                       ; a row with operation_id = 'SUMM' keeping aggregated values 
                                                       ; and many other rows with operation_id equals to some unique operation identifier 
                                                       ; which are evetually aggreagted into the 'SUMM' and deleted
                                                       :value         :int  
                                                       ; counter operation value
                                                       :extra_payload :blob
                                                       ; field for additional data. 
                                                       ; has no specific meaning in the test and exists to tune a single operation slowdown
                                                       :deleted       :boolean
                                                       ; homebrewed tombstone
                                                       :primary-key [:id :operation_id]})
                              (cql/with {:compaction {:class (:compaction-strategy test)}}))))))))

  (invoke! [this test op]
    (let [s (:session conn)
          gc-operator? (gc-operator? (:client-id this) (:gc-operator-client-id test))]
      (c/with-errors op #{:read}
        (alia/execute s (cql/use-keyspace :jepsen_keyspace))
        (case (:f op)
          :add (do (if gc-operator? 
                     (increment-counter-with-gc s 
                                                (:value op) 
                                                test)
                     (increment-counter s 
                                        (next-operation-id this) 
                                        (:value op) 
                                        test))
                   (assoc op :type :ok))
          :read (let [value (read-counter-value s counter-id test)]
                  (assoc op :type :ok :value value))))
      ))

  (close! [_ _]
    (c/close! conn))

  (teardown! [_ _])

  client/Reusable
  (reusable? [_ _] true))

(defn crdt-counter-client
  "A counter implemented using crdt-like datastructure"
  ([] (->CrdtCounterClient (atom false) nil)))

(defn workload
  "An increment-only counter workload."
  [opts] 
  {:client    (crdt-counter-client) 
   :generator (gen/mix [(repeat {:f :add, :value 1}) 
                        (repeat {:f :read})]) 
   :checker   (checker/counter)
   :new-client-id-ctr (atom 0)
   :gc-operator-client-id (atom nil)})
