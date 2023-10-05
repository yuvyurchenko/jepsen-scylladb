(ns scylla.crdt.g-counter-state
  "== OVERVIEW ==
   
   The test is inspired by the idea of Operation-based CRDT / CvRDT Counter.
   Not sure if it is academically accurate to tag this approach as CvRDT, 
   nevertheless, it allows us to build a sophisticated enough workload for more detailed batch testing 
   including atomicity checks, isolation verification, and row deletion/update operations.
   
   == MODEL ==
   
   The counter is modeled as a group of rows within the same partition defined by the ID partitioning key.
   All rows within the partition have the same type and represent a part of whole counter modified only by one client connection.
   The summ of such sub-counters is the total value of the counter.
   This approach allows us to modify the counter concurrently without any external synchronization or LWT or counter types.

   == TEST DETAILS ==
   
   The test itself is not very useful and was created in addition to scylla.crdt.g-counter-operation 
   to ensure that a tombstone-free approach does not trigger 1MB paging 'hidden' threshold.
   Refs:
   https://github.com/scylladb/scylladb/issues/12464
   https://github.com/scylladb/scylladb/blob/973d2a58d0c3af633faa3ce563647a767413b3f8/docs/dev/paged-queries.md
   "
  (:require [clojure.tools.logging :refer [trace error info]]
            [jepsen
             [client    :as client]
             [checker   :as checker]
             [generator :as gen]]
            [qbits.alia :as alia]
            [qbits.alia.policy.retry :as retry]
            [qbits.hayt :as cql]
            [scylla [client :as c]]
            [scylla.utils.payload-generator :as payload-gen]
            [scylla.crdt.g-counter-common :refer :all]
            [qbits.alia.codec :as cdc]
            [slingshot.slingshot :refer [try+ throw+]]))

(defn current-value [records] (:value (first records)))
(defn update-value [inc-value current-value] (+ inc-value current-value))
(def increment-value (fnil update-value 0 0))

(defn increment-counter 
  "The function increments the record owned by the given client-id by the passed value."
  [session client-id inc-value test]
  (let [current-record (execute-read session
                                     test
                                     (cql/select :crdt_g_counters_state
                                                 (cql/where [[= :id counter-id]
                                                             [= :client_id client-id]])))]
    (execute-write session
                   test
                   (cql/insert :crdt_g_counters_state
                               (cql/values [[:id counter-id]
                                            [:client_id client-id]
                                            [:value (increment-value inc-value 
                                                                     (current-value current-record))]
                                            [:extra_payload (payload-gen/generate-payload-bytes (:extra-payload-size test))]])))))

(defn read-counter-value
  "Read total value for the given counter-id considering all writing clients values."
  [session test]
  (let [all-data (execute-read session
                               test
                               (cql/select :crdt_g_counters_state
                                           (cql/where [[= :id counter-id]])))
        counter-value (->> all-data
                           (map :value)
                           (reduce + 0))]
    (trace "READ COUNTER-ID=" counter-id " WITH VALUE=" counter-value ": " all-data)
    counter-value))

(defrecord StateCrdtCounterClient [tbl-created? conn]
  client/Client

  (open! [this test node]
    (assoc this
           :conn (c/open test node)
           :client-id (str "client-" (swap! (:new-client-id-ctr test) inc))))

  (setup! [_ test] 
    (setup-database tbl-created? 
                    conn 
                    test 
                    (cql/create-table :crdt_g_counters_state 
                                      (cql/if-exists false) 
                                      (cql/column-definitions {:id            :int
                                                               ; counter id  
                                                               :client_id     :text
                                                               ; writing client id  
                                                               :value         :int
                                                               ; counter operation value
                                                               :extra_payload :blob
                                                               ; extra payload to slow down processing
                                                               :primary-key [:id :client_id]}) 
                                      (cql/with {:compaction {:class (:compaction-strategy test)}}))))

  (invoke! [this test op]
    (let [s (:session conn)]
      (c/with-errors op #{:read}
        (alia/execute s (cql/use-keyspace :jepsen_keyspace))
        (case (:f op)
          :add (do (increment-counter s
                                      (:client-id this)
                                      (:value op)
                                      test)
                   (assoc op :type :ok))
          :read (let [value (read-counter-value s test)]
                  (assoc op :type :ok :value value))))))

  (close! [_ _]
    (c/close! conn))

  (teardown! [_ _])

  client/Reusable
  (reusable? [_ _] true))

(defn state-crdt-counter-client
  "A counter implemented using crdt-like datastructure"
  ([] (->StateCrdtCounterClient (atom false) nil)))

(defn workload
  "An increment-only counter workload."
  [opts]
  {:client    (state-crdt-counter-client)
   :generator (gen/mix [(repeat {:f :add, :value 1})
                        (repeat {:f :read})])
   :checker   (checker/counter)
   :new-client-id-ctr (atom 0)})
