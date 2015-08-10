(ns hornet.admin
  (:require [taoensso.encore :as encore]
            [hornet.constants :as hc]
            [hornet.conversions :refer [to-bytes to-clojure]])
  (:import [org.apache.hadoop.hbase TableName]
           [org.apache.hadoop.hbase.client Admin HBaseAdmin HConnection]
           [org.apache.hadoop.hbase HColumnDescriptor HTableDescriptor]
           [org.apache.hadoop.hbase.util Bytes]))

(defn make-table-name
  [name]
  (TableName/valueOf ^bytes (to-bytes name)))

(defn table-available?
  [^HConnection connection table-name]
  (.isTableAvailable ^Admin (HBaseAdmin. connection)
                     ^TableName (make-table-name table-name)))

(defn list-table-names
  [^HConnection connection]
  (map to-clojure (.listTableNames ^Admin (HBaseAdmin. connection))))

(defn table-exists?
  [^HConnection connection table-name]
  (.tableExists ^Admin (HBaseAdmin. connection)
                ^TableName (make-table-name table-name)))

(defn table-enabled?
  [^HConnection connection table-name]
  (.isTableEnabled ^Admin (HBaseAdmin. connection)
                   ^TableName (make-table-name table-name)))

(defn table-disabled?
  [^HConnection connection table-name]
  (not (table-enabled? connection table-name)))

(defn column-descriptor
  ([family]
   (column-descriptor family {}))
  ([family {:keys [max-versions
                    min-versions
                    keep-deleted-cells
                    in-memory
                    block-cache-enabled
                    ttl
                    compression-algorithm
                    data-block-encoding
                    bloom-filter-type
                    blocksize
                   replication-scope]}]
   (encore/doto-cond [_ (HColumnDescriptor. (to-bytes family))]
                     max-versions (.setMaxVersions max-versions)
                     min-versions (.setMinVersions min-versions)
                     keep-deleted-cells (.setKeepDeletedCells (hc/keep-deleted-cells keep-deleted-cells))
                     in-memory (.setInMemory in-memory)
                     block-cache-enabled (.setBlockCacheEnabled block-cache-enabled)
                     ttl (.setTimeToLive ttl)
                     data-block-encoding (.setDataBlockEncoding
                                          (hc/data-block-encoding data-block-encoding))
                     compression-algorithm (.setCompressionType
                                            (hc/compression-algorithm compression-algorithm))
                     bloom-filter-type (.setBloomFilterType
                                        (hc/bloom-filter-type bloom-filter-type))
                     replication-scope (.setScope (hc/replication-scope replication-scope))
                     blocksize (.setBlocksize blocksize))))

(defprotocol ColumnDescriptor
  (to-column-descriptor [this]))

(extend-protocol ColumnDescriptor
  clojure.lang.MapEntry
  (to-column-descriptor [[family settings]]
    (column-descriptor family settings))

  clojure.lang.Keyword
  (to-column-descriptor [family]
    (column-descriptor family))

  java.lang.String
  (to-column-descriptor [family]
    (column-descriptor family)))

(defn create-table
  [^HConnection connection table-name column-families]
  (let [table (HTableDescriptor. ^TableName (make-table-name table-name))]
    (doseq [cdescriptor (map to-column-descriptor column-families)]
      (.addFamily table cdescriptor))
    (.createTable (HBaseAdmin. connection) table)))

(defn disable-table
  [^HConnection connection table-name]
  (.disableTable ^Admin (HBaseAdmin. connection)
                 ^TableName (make-table-name table-name)))

(defn enable-table
  [^HConnection connection table-name]
  (.enableTable ^Admin (HBaseAdmin. connection)
                ^TableName (make-table-name table-name)))

(defn delete-table
  [^HConnection connection table-name]
  (.deleteTable ^Admin (HBaseAdmin. connection)
                ^TableName (make-table-name table-name)))

(defn remove-table
  [^HConnection connection table-name]
  (when (table-exists? connection table-name)
    (when (table-enabled? connection table-name)
      (disable-table connection table-name))
    (delete-table connection table-name)))

(defn truncate-table
  ([^HConnection connection table-name]
   (truncate-table connection table-name true))
  ([^HConnection connection table-name preserve-splits]
   (.truncateTable ^Admin (HBaseAdmin. connection)
                   ^TableName (make-table-name table-name)
                   preserve-splits)))
