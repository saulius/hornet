(ns hornet.test-helpers
  (:require [hornet.client :as client]
            [hornet.admin :as admin]))

(defn make-test-connection
  []
  (client/connection (client/make-config {:hbase.zookeeper.quorum "127.0.0.1"})))

(defn create-test-table
  ([table-name families]
   (let [connection (make-test-connection)]
     (admin/remove-table connection table-name)
     (admin/create-table connection table-name families)))
  ([table-name families test]
   (let [connection (make-test-connection)]
     (create-test-table table-name families)
     (test)
     (admin/remove-table connection table-name))))

(defn truncate-tables
  [table-names]
  (let [connection (make-test-connection)]
    (doseq [table-name table-names]
      (admin/disable-table connection table-name)
      (admin/truncate-table connection table-name))))
