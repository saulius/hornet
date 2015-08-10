(ns hornet.client
  (:refer-clojure :rename {get map-get})
  (:require [taoensso.encore :as encore]
            [hornet.requests :as req]
            [hornet.conversions :as conv]
            [hornet.table :as ht]
            [hornet.admin :as admin])
  (:import [org.apache.hadoop.conf Configuration]
           [org.apache.hadoop.hbase HBaseConfiguration
            HConstants
            KeyValue
            TableName]
           [org.apache.hadoop.hbase.client ResultScanner
            HConnectionManager
            HConnection
            HTablePool
            Durability
            Get
            Increment
            Put
            Delete
            Scan
            Result
            HTableInterface
            Operation]
           [org.apache.hadoop.hbase.filter CompareFilter
            CompareFilter$CompareOp]
           [org.apache.hadoop.hbase.util Bytes]
           [org.apache.hadoop.hbase TableName]))

(defn make-config
  "Constructs a default HBaseConfiguration object and sets the options given in
   config-map.
   Example: (make-config
              {\"hbase.zookeeper.dns.interface\" \"lo\"
              :hbase.zookeeper.quorum \"127.0.0.1\"})"
  [config-map]
  (let [config-obj (HBaseConfiguration/create)]
    (doseq [[option value] (seq config-map)]
      (.set config-obj (name option) (name value)))
    config-obj))

(defn connection
  [^Configuration config]
  (HConnectionManager/createConnection config))

(defprotocol IHBaseRequest
  (execute [^Operation request ^HTableInterface table]))

;; TODO move with-open statements here from put/get fns?
(extend-protocol IHBaseRequest
  Get
  (execute [^Get request ^HTableInterface table]
    (let [result (.. table (get request) (rawCells))]
      (map conv/to-clojure result)))

  Scan
  (execute [^Scan request ^HTableInterface table]
    (with-open [^ResultScanner result-scanner (.getScanner table request)]
      ;; TODO is it ok for lazy seq?
      (conv/to-clojure (seq result-scanner))))

  Put
  (execute [^Put request ^HTableInterface table]
    (.put table request))

  Delete
  (execute [^Delete request ^HTableInterface table]
    (.delete table request))

  Increment
  (execute [^Increment request ^HTableInterface table]
    (.increment table request)))

(defn get
  ([^HConnection conn table-name rowkey]
   (get conn table-name rowkey {}))
  ([^HConnection conn table-name rowkey args]
   (with-open [^HTableInterface table (ht/table conn table-name)]
     (let [request (req/get rowkey args)]
       (execute request table)))))

(defn put
  ([^HConnection conn table-name rowkey columns]
   (put conn table-name rowkey columns {}))
  ([^HConnection conn table-name rowkey columns args]
   (with-open [^HTableInterface table (ht/table conn table-name)]
     (let [request (req/put rowkey columns args)]
       (execute request table)))))

(defn delete
  ([^HConnection conn table-name rowkey]
   (delete conn table-name rowkey {}))
  ([^HConnection conn table-name rowkey args]
   (with-open [^HTableInterface table (ht/table conn table-name)]
     (let [request (req/delete rowkey args)]
       (execute request table)))))

(defn increment
  ([^HConnection conn table-name rowkey columns]
   (increment conn table-name rowkey columns {}))
  ([^HConnection conn table-name rowkey columns args]
   (with-open [^HTableInterface table (ht/table conn table-name)]
     (let [request (req/increment rowkey columns args)]
       (execute request table)))))

(defn scan
  ([^HConnection connection table-name]
   (scan connection table-name {}))
  ([^HConnection connection table-name query]
   (with-open [^HTableInterface table (ht/table connection table-name)]
     (let [request (req/scan query)]
       (execute request table)))))
