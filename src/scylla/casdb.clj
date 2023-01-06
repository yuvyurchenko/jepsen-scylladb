(ns scylla.casdb 
  (:require [clojure [string :as str]] 
            [clojure.tools.logging :refer [info]] 
            [scylla 
             [client :as sc] 
             [db :as scdb]] 
            [jepsen 
             [db        :as db] 
             [control   :as c :refer [| lit]] 
             [util      :as util :refer [meh timeout]]] 
            [jepsen.os.debian :as debian] 
            [jepsen.control [net :as net]]))

(defn cached-install?
  [src]
  (try (c/exec :grep :-s :-F :-x (lit src) (lit ".download"))
       true
       (catch RuntimeException _ false)))

(defn install-jdk8!
  "Scylla has a mandatory dep on jdk8, which isn't normally available in Debian
  Buster."
  []
  (info "installing JDK8")
  (c/su
   (c/exec :wget :-qO :- "https://adoptopenjdk.jfrog.io/adoptopenjdk/api/gpg/key/public" | :apt-key :add :-)
   (debian/add-repo! "adoptopenjdk" "deb  [arch=amd64] https://adoptopenjdk.jfrog.io/adoptopenjdk/deb/ buster main")
   (debian/install [:adoptopenjdk-8-hotspot])))

(defn install!
  "Installs Cassandra on the given node."
  [node version]
  (c/su
   (c/cd
    "/tmp"
    (let [tpath (System/getenv "CASSANDRA_TARBALL_PATH")
          url (or tpath
                  (System/getenv "CASSANDRA_TARBALL_URL")
                  (str "https://archive.apache.org/dist/cassandra/" version
                       "/apache-cassandra-" version "-bin.tar.gz"))]
      (info node "installing Cassandra from" url)
      (if (cached-install? url)
        (info "Used cached install on node" node)
        (do (if tpath
              (c/upload tpath "/tmp/cassandra.tar.gz")
              (c/exec :wget :-O "cassandra.tar.gz" url (lit ";")))
            (c/exec :tar :xzvf "cassandra.tar.gz" :-C "~")
            (c/exec :rm :-r :-f (lit "~/cassandra"))
            (c/exec :mv (lit "~/apache* ~/cassandra"))
            (c/exec :echo url :> (lit ".download")))))
    
    (install-jdk8!))))

(defn configure!
  "Uploads configuration files to the given node."
  [node test]
  (info node "configuring Cassandra")
  (c/su
   (doseq [rep ["\"s/#MAX_HEAP_SIZE=.*/MAX_HEAP_SIZE='512M'/g\""
                "\"s/#HEAP_NEWSIZE=.*/HEAP_NEWSIZE='128M'/g\""
                "\"s/LOCAL_JMX=yes/LOCAL_JMX=no/g\""
                (str "'s/# JVM_OPTS=\"$JVM_OPTS -Djava.rmi.server.hostname="
                     "<public name>\"/JVM_OPTS=\"$JVM_OPTS -Djava.rmi.server.hostname="
                     (name node) "\"/g'")
                (str "'s/JVM_OPTS=\"$JVM_OPTS -Dcom.sun.management.jmxremote"
                     ".authenticate=true\"/JVM_OPTS=\"$JVM_OPTS -Dcom.sun.management"
                     ".jmxremote.authenticate=false\"/g'")
                "'/JVM_OPTS=\"$JVM_OPTS -Dcassandra.mv_disable_coordinator_batchlog=.*\"/d'"]]
     (c/exec :sed :-i (lit rep) "~/cassandra/conf/cassandra-env.sh"))
   (doseq [rep (into ["\"s/cluster_name: .*/cluster_name: 'jepsen'/g\""
                      "\"s/row_cache_size_in_mb: .*/row_cache_size_in_mb: 20/g\""
                      "\"s/seeds: .*/seeds: 'n1,n2'/g\""
                      (str "\"s/listen_address: .*/listen_address: " node
                           "/g\"")
                      (str "\"s/rpc_address: .*/rpc_address: " (scdb/dns-resolve node) "/g\"")
                      (str "\"s/broadcast_rpc_address: .*/broadcast_rpc_address: "
                           (net/local-ip) "/g\"")
                      "\"s/internode_compression: .*/internode_compression: none/g\""
                      (str "\"s/hinted_handoff_enabled:.*/hinted_handoff_enabled: "
                           (:hinted-handoff test) "/g\"")
                      "\"s/commitlog_sync: .*/commitlog_sync: batch/g\""
                      (str "\"s/# commitlog_sync_batch_window_in_ms: .*/"
                           "commitlog_sync_batch_window_in_ms: 1.0/g\"")
                      "\"s/commitlog_sync_period_in_ms: .*/#/g\""
                      (str "\"s/# phi_convict_threshold: .*/phi_convict_threshold: " (:phi-level test)
                           "/g\"")
                      "\"s/auto_bootstrap: .*/auto_bootstrap: true/g\""])]
     (c/exec :sed :-i (lit rep) "~/cassandra/conf/cassandra.yaml"))
   (c/exec :echo (str "JVM_OPTS=\"$JVM_OPTS -Dcassandra.mv_disable_coordinator_batchlog="
                      (not (:coordinator-batchlog test)) "\"")
           :>> "~/cassandra/conf/cassandra-env.sh")
   (c/exec :echo "JVM_OPTS=\"$JVM_OPTS -Dcassandra.consistent.rangemovement=false\""
           :>> "~/cassandra/conf/cassandra-env.sh")))

(defn start!
  "Starts Cassandra."
  [node test]
  (info node "starting Cassandra")
  (c/su
   (c/exec (lit "~/cassandra/bin/cassandra -R")))
  (sc/close! (sc/await-open test node))
  (info node "started Cassandra"))

(defn stop!
  "Stops Cassandra."
  [node]
  (info node "stopping Cassandra")
  (c/su
   (meh (c/exec :killall :java))
   (while (.contains (c/exec :ps :-ef) "java")
     (Thread/sleep 100)))
  (info node "has stopped Cassandra"))

(defn wipe!
  "Shuts down Cassandra and wipes data."
  [node]
  (stop! node)
  (info node "deleting data files")
  (c/su
   (meh (c/exec :rm :-r "~/cassandra/logs"))
   (meh (c/exec :rm :-r "~/cassandra/data/data"))
   (meh (c/exec :rm :-r "~/cassandra/data/hints"))
   (meh (c/exec :rm :-r "~/cassandra/data/commitlog"))
   (meh (c/exec :rm :-r "~/cassandra/data/saved_caches"))))

(defn db
  "Sets up and tears down CassandraDB"
  [version]
  (let [tcpdump (db/tcpdump {:ports         [9042]
                             :clients-only? true})]
    (reify db/DB
      (setup! [db test node]
        (when (:trace-cql test) (sc/start-tracing! test))
        (db/setup! tcpdump test node)
        (doto node 
          (install! version) 
          (configure! test) 
          (start! test)))
      
      (teardown! [db test node] 
        (wipe! node))
      
      db/LogFiles
      (log-files [db test node] 
                 (concat (db/log-files tcpdump test node) 
                         ["~/cassandra/logs/system.log"])))))