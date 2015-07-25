(ns hornet.conversions
  (:refer-clojure :rename {get map-get
                           flush clj-flush})
  (:require [hornet.base :as base])
  (:import [org.apache.hadoop.hbase.util Bytes]
           [org.apache.hadoop.hbase Cell KeyValue TableName]
           [org.apache.hadoop.hbase.client Durability Result]))

(defprotocol HBaseBytesRepresentation
  (^bytes to-bytes [this]))

(extend-protocol HBaseBytesRepresentation
  java.lang.String
  (to-bytes [this]
    (Bytes/toBytes ^String this))

  clojure.lang.Keyword
  (to-bytes [this]
    (to-bytes (name this)))

  java.lang.Long
  (to-bytes [this]
    (Bytes/toBytes ^long this)))

(defprotocol ClojureRepresentation
  (to-clojure [this]))

(extend-protocol ClojureRepresentation
  Result
  (to-clojure [this]
    (map to-clojure (.rawCells this)))

  Durability
  (to-clojure [this]
    ((clojure.set/map-invert base/durability-map) this))

  KeyValue
  (to-clojure [this]
      {:key (.getKey this)
       :family (.getFamily this)
       :qualifier (.getQualifier this)
       :timestamp (.getTimestamp this)
       :value (.getValue this)})

  TableName
  (to-clojure [this]
    (.toString this))

  java.lang.Long
  (to-clojure [this]
    this)

  java.lang.String
  (to-clojure [this]
    this)

  java.lang.Boolean
  (to-clojure [this]
    this)

  java.util.Collection
  (to-clojure [this]
    (map to-clojure this))

  ;; TODO this is nice for Scan but not ok otherwise?
  nil
  (to-clojure [this]
    []))

;; living outside of extend-protocol until http://dev.clojure.org/jira/browse/CLJ-1381
;; is resolved
(extend-type (Class/forName "[Lorg.apache.hadoop.hbase.Cell;")
  ClojureRepresentation
  (to-clojure [this]
    (map to-clojure this)))

(extend-type (Class/forName "[B")
  ClojureRepresentation
  (to-clojure [this]
    (Bytes/toString this)))
