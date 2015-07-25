(ns hornet.base
  (:import [org.apache.hadoop.hbase HConstants KeepDeletedCells]
           [org.apache.hadoop.hbase.io.compress Compression$Algorithm]
           [org.apache.hadoop.hbase.io.encoding DataBlockEncoding]
           [org.apache.hadoop.hbase.regionserver BloomType]
           [org.apache.hadoop.hbase.client Durability]))

(def durability-map
  {:async-wal Durability/ASYNC_WAL
   :fsync-wal Durability/FSYNC_WAL
   :skip-wal  Durability/SKIP_WAL
   :sync-wal  Durability/SYNC_WAL
   :default   Durability/USE_DEFAULT})

(defn encode-durability
  [durability]
  (durability durability-map))

(def compression-map
  {:lzo    Compression$Algorithm/LZO
   :gz     Compression$Algorithm/GZ
   :none   Compression$Algorithm/NONE
   :snappy Compression$Algorithm/SNAPPY
   :lz4    Compression$Algorithm/LZ4})

(def data-block-encoding-map
  {:none        DataBlockEncoding/NONE
   :prefix      DataBlockEncoding/PREFIX
   :diff        DataBlockEncoding/DIFF
   :fast-diff   DataBlockEncoding/FAST_DIFF
   :prefix-tree DataBlockEncoding/PREFIX_TREE})

(def bloom-filter-type-map
  {:none   BloomType/NONE
   :row    BloomType/ROW
   :rowcol BloomType/ROWCOL})

(def replication-scope-map
  {:local  HConstants/REPLICATION_SCOPE_LOCAL
   :global HConstants/REPLICATION_SCOPE_GLOBAL})

(def ^KeepDeletedCells keep-deleted-cells-map
  {:false KeepDeletedCells/FALSE
   :true  KeepDeletedCells/TRUE
   :ttl   KeepDeletedCells/TTL})
