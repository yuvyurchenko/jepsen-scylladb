(ns scylla.batch-crosstable
  "The idea of the test is to check atomicity property for multi-table single-partition batches.
   Single-partition is a bit vague term. In this test by single-partition we assume 
   rows with the same partitiong key values within the same keysapce, 
   but not necessary from the same tables."
  (:require [clojure [pprint :refer :all]]
            [clojure.tools.logging :refer [info debug]]
            [jepsen
             [client    :as client]
             [checker   :as checker]
             [generator :as gen]
             [nemesis   :as nemesis]]
            [scylla [client :as c]]
            [scylla.utils.payload-generator :as payload-gen]
            [qbits.alia :as alia]
            [qbits.hayt :as cql]))

(defn table-name [table-num] (keyword (str "batch_crosstable_" table-num)))
(defn all-tables [test] (mapv table-name (range (:tables-number test))))

(defn create-batch-statement 
  "Creates a batch inserting the same key-value pairs into multiple tables." 
  [test value]
  (cql/batch
   (apply cql/queries
          (->> (all-tables test)
               (map #(cql/insert %
                                 (cql/values [[:key value]
                                              [:value value]
                                              [:extra_payload (payload-gen/generate-payload-bytes (:extra-payload-size test))]])))))))

(defrecord CrosstableBatchSetClient [tbl-created? conn]
  client/Client

  (open! [this test node]
    (assoc this :conn (c/open test node)))

  (setup! [_ test]
    (let [s (:session conn)]
      (locking tbl-created?
        (when (compare-and-set! tbl-created? false true)
          (c/retry-each
           (alia/execute s (cql/create-keyspace
                            :jepsen_keyspace
                            (cql/if-exists false)
                            (cql/with {:replication {:class :SimpleStrategy 
                                                     :replication_factor (:replication-factor test)}})))
           (alia/execute s (cql/use-keyspace :jepsen_keyspace))
           (doseq [table (all-tables test)] 
                  (alia/execute s (cql/create-table 
                                   table
                                   (cql/if-exists false) 
                                   (cql/column-definitions {:key           :int 
                                                            :value         :int 
                                                            :extra_payload :blob
                                                            :primary-key [:key]}) 
                                   (cql/with {:compaction {:class (:compaction-strategy test)}})))))))))

  (invoke! [this test op]
    (let [s (:session conn)]
      (c/with-errors op #{:read}
        (alia/execute s (cql/use-keyspace :jepsen_keyspace))
        (case (:f op)
          :add (let [batch-stmt (create-batch-statement test (:value op))]
                 (debug "apply operation in batch" batch-stmt)
                 (alia/execute s
                               batch-stmt
                               (merge {:consistency :quorum}
                                      (c/write-opts test)))
                 (assoc op :type :ok))
          :read (do (info "Final read operation cooldown...")
                    ;; we give some extra time to repair before the final read 
                    (Thread/sleep 15000)
                    (info "Final read operation execution")
                    ;; we must use ALL consistency, because in case of failure a change can be inserted only into a single node.
                    ;; eventually it will be repaired either by read repair or hinted handoff, 
                    ;; but until then it may cause a discrepancy in tables read results, 
                    ;; because they are independent and the outcomes would depend on the first answered replicas 
                    ;; in case of N independent reads even for the QUORUM consistency.
                    (let [results (->> (all-tables test)
                                       (map #(->> (alia/execute s
                                                                (cql/select %)
                                                                (merge {:consistency :all}
                                                                       (c/read-opts test)))
                                                  (map :value)
                                                  (into (sorted-set)))))]
                      (if-not (apply = results) 
                        (assoc op :type :fail :value results) 
                        (assoc op :type :ok :value (first results)))))))))

  (close! [_ _]
    (c/close! conn))

  (teardown! [_ _])

  client/Reusable
  (reusable? [_ _] true))

(defn batch-set-client
  "A set implemented using batched inserts"
  []
  (->CrosstableBatchSetClient (atom false) nil))

(defn set-workload
  [opts]
  {:client          (batch-set-client)
   :generator       (->> (range)
                         (map (fn [x] {:type :invoke, :f :add, :value x})))
   :final-generator {:f :read}
   :checker         (checker/set)
   :tables-number   10})

