(ns scylla.crdt.g-counter-common
  (:require [clojure.tools.logging :refer [error]]
            [qbits.alia :as alia]
            [qbits.alia.policy.retry :as retry]
            [qbits.hayt :as cql]
            [scylla [client :as c]]
            [qbits.alia.codec :as cdc])
(:import (java.util List)))

(def counter-id 0)

(defn execute-write
  "Execute write CQL statement"
  [session test statement]
  (let [params (merge {:consistency :quorum
                       :retry-policy  (retry/fallthrough-retry-policy)}
                      (c/write-opts test))]
    (alia/execute session statement params)))

(defn eager-result-set-fn
  "We use it to detect multi-paging result sets 
   since they violates single-partition isolation 
   and cause this test failure
   
   It turned out it is not so easy to correctly handle an exception thrown in the result-set-fn 
   since it is executed somewhere deep inside the library.
   As a workaround we use an atom to pass the multipaging event detection back to the test 
   and wrap other internals into a try-catch block for debug purpouses"
  [multipage-flag?]
  (fn [rs]
    (let [all-rows (into [] rs)
          exec-info (cdc/execution-info rs)
          multi-paging? (> (.size exec-info) 1)]
      (when multi-paging?
        (try
          (error "Multipaging detected with resultset size:" (count all-rows)
                 "Execution info:" (->> exec-info
                                        (map #(str "ExecInfo{"
                                                   "pagingState=" (.getPagingState %)
                                                   ";warnings=" (.getWarnings %)
                                                   ";payload=" (.getIncomingPayload %)
                                                   ";speculativeCnt=" (.getSpeculativeExecutions %)
                                                   ";successIdx=" (.getSuccessfulExecutionIndex %)
                                                   "}"))
                                        (reduce str "")))
          (catch Exception e (error "Failed to log" e)))
        (reset! multipage-flag? true))
      all-rows)))

(defn execute-read
  "Execute read CQL statement"
  [session test statement]
  (let [multipage-flag? (atom false)
        params (merge {:consistency :quorum
                       :retry-policy (retry/fallthrough-retry-policy)
                       :result-set-fn (eager-result-set-fn multipage-flag?)}
                      (c/read-opts test))
        results (alia/execute session statement params)]
    ;; comment the throw to make the test fail if multi-paging happens
    ;; 
    ;; (when @multipage-flag?
    ;;   (throw+ {:type       :multipaging-error 
    ;;            :message    "Multipaging violates operation isolation" 
    ;;            :definite?  false}))
    results))

(defn setup-database 
  "Common db schema setup routines."
  [tbl-created? conn test create-table-statement]
  (let [s (:session conn)
        update-system-config (:update-system-config test)] 
    (locking tbl-created? 
      (when (compare-and-set! tbl-created? false true) 
        (c/retry-each 
         (alia/execute s (cql/create-keyspace 
                          :jepsen_keyspace 
                          (cql/if-exists false) 
                          (cql/with {:replication {:class :SimpleStrategy 
                                                   :replication_factor (:replication-factor test)}}))) 
         (alia/execute s (cql/use-keyspace :jepsen_keyspace))
         (alia/execute s create-table-statement)
         (doseq [[k v] update-system-config] 
           (alia/execute s (cql/update :system.config 
                                       (cql/set-columns {:value v}) 
                                       (cql/where [[= :name k]])))))))))
