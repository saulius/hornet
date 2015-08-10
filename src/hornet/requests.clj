(ns hornet.requests
  (:refer-clojure :rename {get map-get})
  (:require [taoensso.encore :as encore]
            [hornet.constants :as hc]
            [hornet.utils :as hu]
            [hornet.conversions :refer [to-bytes to-clojure]])
  (:import [org.apache.hadoop.hbase.client Durability HTablePool Get Put Row Delete Scan
  Increment Mutation Result HTableInterface]
           [org.apache.hadoop.hbase.filter Filter]
           [org.apache.hadoop.hbase HBaseConfiguration HConstants KeyValue]
           [org.apache.hadoop.hbase HConstants]))

(defprotocol IRequest
  (row [this]))

(defprotocol IIdentifiableRequest
  (set-id [this])
  (id [this]))

(defprotocol ITimestampedRequest
  (min-timestamp [this])
  (max-timestamp [this])
  (time-range [this]))

(defprotocol IPutRequest
  (has? [this family qualifier]
    [this family qualifier value]
    [this family qualifier ts value]))

(defprotocol IGetRequest
  (versions [this])
  (columns [this])
  (families [this]))

(defprotocol IScanRequest
  (start-row [this])
  (stop-row [this])
  (max-versions [this])
  (batch-size [this])
  (caching? [this])
  (reversed? [this])
  (small? [this])
  (load-column-families-on-demand? [this])
  (max-result-size [this]))

(defprotocol IMutationRequest
  (set-durability [this])
  (durability [this])
  (set-ttl [this])
  (ttl [this]))

(defprotocol IQueryRequest
  (store-limit [this])
  (store-offset [this])
  (cache-blocks? [this]))

(extend-protocol IRequest
  Row
  (row [this] (to-clojure (.getRow this))))

(extend-type Put
  IPutRequest
  (has?
    ([this family qualifier]
     (.has this (to-bytes family) (to-bytes qualifier)))
    ([this family qualifier value]
     (.has this ^bytes (to-bytes family) ^bytes (to-bytes qualifier) ^bytes (to-bytes value)))
    ([this family qualifier ts value]
     (.has this (to-bytes family) (to-bytes qualifier) ts (to-bytes value))))

  IIdentifiableRequest
  (set-id [^Put this id] (.setId this id))
  (id [this] (.getId this))

  IMutationRequest
  (ttl [this] (.getTTL this))
  (durability [this] (to-clojure (.getDurability this))))

(extend-type Delete
  IIdentifiableRequest
  (set-id [^Delete this id] (.setId this id))
  (id [this] (.getId this))

  IMutationRequest
  (ttl [this] (.getTTL this))
  (durability [this] (to-clojure (.getDurability this))))

(extend-type Get
  IGetRequest
  (versions [this] (.getMaxVersions this))
  (columns [this]
    (let [map (.getFamilyMap this)
          reducer-fn (fn [memo [family columns]]
                       (let [fam (to-clojure family)
                             cols (mapv to-clojure (seq columns))]
                         (assoc memo fam cols)))]
      (reduce reducer-fn {} map)))
  (families [this] (map to-clojure (keys (.getFamilyMap this))))

  IQueryRequest
  (store-limit [this] (.getMaxResultsPerColumnFamily this))
  (store-offset [this] (.getRowOffsetPerColumnFamily this))
  (cache-blocks? [this] (.getCacheBlocks this))

  ITimestampedRequest
  (min-timestamp [this] (:min-timestamp (time-range this)))
  (max-timestamp [this] (:max-timestamp (time-range this)))

  (time-range [this]
    (let [time-range (.getTimeRange this)]
      {:min-timestamp (.getMin time-range)
       :max-timestamp (.getMax time-range)})))

(extend-type Scan
  IScanRequest
  (start-row [this] (to-clojure (.getStartRow this)))
  (stop-row [this] (to-clojure (.getStopRow this)))
  (max-versions [this] (.getMaxVersions this))
  (batch-size [this] (.getBatch this))
  (caching? [this] (.getCaching this))
  (reversed? [this] (.isReversed this))
  (small? [this] (.isSmall this))
  (load-column-families-on-demand? [this] (.getLoadColumnFamiliesOnDemandValue this))
  (max-result-size [this] (.getMaxResultSize this))

  IQueryRequest
  (store-limit [this] (.getMaxResultsPerColumnFamily this))
  (store-offset [this] (.getRowOffsetPerColumnFamily this))
  (cache-blocks? [this] (.getCacheBlocks this))

  ITimestampedRequest
  (min-timestamp [this] (:min-timestamp (time-range this)))
  (max-timestamp [this] (:max-timestamp (time-range this)))
  (time-range [this]
    (let [time-range (.getTimeRange this)]
      {:min-timestamp (.getMin time-range)
       :max-timestamp (.getMax time-range)})))

(extend-type Increment
  ITimestampedRequest
  (min-timestamp [this] (:min-timestamp (time-range this)))
  (max-timestamp [this] (:max-timestamp (time-range this)))
  (time-range [this]
    (let [time-range (.getTimeRange this)]
      {:min-timestamp (.getMin time-range)
       :max-timestamp (.getMax time-range)})))

(defn get-families
  [^Get request families]
  (doseq [family families]
    (.addFamily request (to-bytes family))))

;; Same method as delete-columns. Get and Delete needs the same interface?
(defn get-columns
  [^Get request cols]
  (doseq [[family columns] cols
          column columns]
    (.addColumn request (to-bytes family) (to-bytes column))))

(defn ^Get get
  ([rowkey]
   (get rowkey {}))
  ([rowkey {:keys [columns
                   families
                   min-timestamp
                   max-timestamp
                   timestamp
                   filter
                   all-versions
                   max-versions
                   store-limit
                   store-offset
                   cache-blocks]}]
   (encore/doto-cond [_ (Get. ^bytes (to-bytes rowkey))]
                     columns      (get-columns columns)
                     families     (get-families families)
                     max-versions (.setMaxVersions max-versions)
                     all-versions (.setMaxVersions)
                     timestamp    (.setTimeStamp timestamp)
                     filter       (.setFilter ^Filter filter)
                     (not (nil? cache-blocks)) (.setCacheBlocks cache-blocks)
                     store-limit  (.setMaxResultsPerColumnFamily store-limit)
                     store-offset (.setRowOffsetPerColumnFamily store-offset)
                     (and min-timestamp
                          max-timestamp) (.setTimeRange min-timestamp max-timestamp))))

(defn- put-columns
  [^Put request cells]
  (doseq [[[family column ^long timestamp] value] (hu/flatten-map cells)]
    (.addColumn request
                ^bytes (to-bytes family)
                ^bytes (to-bytes column)
                (or timestamp HConstants/LATEST_TIMESTAMP)
                ^bytes (to-bytes value))))

;; TODO visibility
;; TODO attribute?
(defn ^Put put
  ([rowkey cells]
   (put rowkey cells {}))
  ([rowkey cells {:keys [durability id ttl]}]
   (encore/doto-cond [_ (Put. (to-bytes rowkey))]
                     :always    (put-columns cells)
                     durability (.setDurability (hc/durability durability))
                     id         (.setId id)
                     ttl        (.setTTL ttl))))

(defn scan-families
  [^Scan request families]
  (doseq [family families]
    (.addFamily request (to-bytes family))))

(defn scan-columns
  [^Scan request cols]
  (doseq [[family columns] cols
          column columns]
    (.addColumn request (to-bytes family) (to-bytes column))))

(defn ^Scan scan
  [{:keys [filter
           start-row
           stop-row
           max-versions
           batch-size
           store-limit
           store-offset
           caching
           max-result-size
           cache-blocks
           load-column-families-on-demand
           min-timestamp
           max-timestamp
           reversed
           small
           families
           columns]}]
  (encore/doto-cond [scan (Scan.)]
                    filter (.setFilter ^Filter filter)
                    start-row (.setStartRow (to-bytes start-row))
                    stop-row (.setStopRow (to-bytes stop-row))
                    max-versions (.setMaxVersions max-versions)
                    batch-size (.setBatch batch-size)
                    store-limit (.setMaxResultsPerColumnFamily store-limit)
                    store-offset (.setRowOffsetPerColumnFamily store-offset)
                    caching (.setCaching caching)
                    (not (nil? cache-blocks)) (.setCacheBlocks cache-blocks)
                    max-result-size (.setMaxResultSize max-result-size)
                    load-column-families-on-demand (.setLoadColumnFamiliesOnDemand
                                                    load-column-families-on-demand)
                    reversed (.setReversed reversed)
                    small (.setSmall small)
                    families (scan-families families)
                    columns (scan-columns columns)
                    (and min-timestamp
                         max-timestamp) (.setTimeRange min-timestamp max-timestamp)))

(defn delete-families
  [^Delete request families]
  (doseq [family families]
    (.addFamily request (to-bytes family))))

(defn delete-columns
  [^Delete request cols]
  (doseq [[family columns] cols
          column columns]
    (.addColumn request (to-bytes family) (to-bytes column))))

(defn ^Delete delete
  ([rowkey]
   (delete rowkey {}))
  ([rowkey {:keys [timestamp
                   family
                   families
                   columns
                   id
                   durability
                   ttl]}]
   (encore/doto-cond [_ (Delete. (to-bytes rowkey))]
                     family     (delete-families [family])
                     families   (delete-families families)
                     timestamp  (.setTimestamp timestamp)
                     columns    (delete-columns columns)
                     id         (.setId id)
                     durability (.setDurability (hc/durability durability))
                     ttl        (.setTTL ttl))))

(defn increment-columns
  [^Increment request cols]
  (doseq [[family columns] cols
          [column amount] columns]
    (.addColumn request (to-bytes family) (to-bytes column) amount)))

(defn ^Increment increment
  ([rowkey amount]
   (increment rowkey columns {}))
  ([rowkey columns {:keys [min-timestamp
                           max-timestamp]}]
   (encore/doto-cond [_ (Increment. (to-bytes rowkey))]
                     :always (increment-columns columns)
                     (and min-timestamp
                          max-timestamp) (.setTimeRange min-timestamp max-timestamp))))
