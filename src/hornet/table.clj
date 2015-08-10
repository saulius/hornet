(ns hornet.table
  (:require [hornet.conversions :as conv])
  (:import [org.apache.hadoop.hbase TableName]
           [org.apache.hadoop.hbase.client HConnection]))

(defn table-name
  [name]
  (TableName/valueOf ^bytes (conv/to-bytes name)))

(defn table
  [^HConnection connection name]
  (.getTable connection ^TableName (table-name name)))
