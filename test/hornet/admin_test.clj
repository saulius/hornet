(ns hornet.admin-test
  (:require [clojure.test :refer :all]
            [clojure.set :refer [subset?]]
            [hornet.test-helpers :as th]
            [hornet.admin :refer :all]))

(let [connection (th/make-test-connection)]
  (deftest table-available?-test
    (th/create-test-table :hornet-test-table [:family]
      (fn []
        (testing "with available table"
          (is (= true (table-available? connection :hornet-test-table))))

        (testing "with unavailable table"
          (is (= false (table-available? connection :other-test-table)))))))

  (deftest list-table-names-test
    (th/create-test-table :hornet-test-table [:family]
      (fn []
        (let [result (set (list-table-names connection))]
          (is (true? (subset? #{"hornet-test-table"} result)))
          (is (false? (subset? #{"no-such-table"} result)))))))

  (deftest table-exists?-test
    (th/create-test-table :hornet-test-table [:family]
      (fn []
        (testing "with existing table"
          (is (true? (table-exists? connection :hornet-test-table))))

        (testing "with non existing table"
          (is (false? (table-exists? connection :should-not-exist)))))))

  (deftest table-enabled?-test
    (th/create-test-table :hornet-test-table [:family]
      (fn []
        (th/create-test-table :hornet-disabled-test-table [:family]
          (fn []
            (testing "with enabled table"
              (is (true? (table-enabled? connection :hornet-test-table))))

            (testing "with disabled table"
              (disable-table connection :hornet-disabled-test-table)
              (is (false? (table-enabled? connection :hornet-disabled-test-table))))

            (testing "with non existent table"
              (is (thrown? org.apache.hadoop.hbase.TableNotFoundException
                           (table-enabled? connection :non-existent-test-table)))))))))

  (deftest table-disabled?-test
    (th/create-test-table :hornet-test-table [:family]
      (fn []
        (th/create-test-table :hornet-disabled-test-table [:family]
          (fn []
            (disable-table connection :hornet-disabled-test-table)
            (is (false? (table-disabled? connection :hornet-test-table)))
            (is (true? (table-disabled? connection :hornet-disabled-test-table)))
            (is (thrown? org.apache.hadoop.hbase.TableNotFoundException
                         (table-enabled? connection :non-existent-test-table))))))))

  (deftest create-table-test
    (testing "table with default column family options"
      (create-table connection :hornet-test-table [:family])
      (is (true? (table-enabled? connection :hornet-test-table)))
      (remove-table connection :hornet-test-table))

    (testing "sets custom column family options if passed"
      (create-table connection
                    :hornet-test-table
                    {:family {:max-versions 1
                              :min-versions 3
                              :keep-deleted-cells :true
                              :in-memory true
                              :block-cache-enabled true
                              :ttl 23423
                              :compression-type :gz
                              :data-block-encoding :fast-diff
                              :bloom-filter-type :row
                              :blocksize 4096
                              :scope :global}})
      (is (true? (table-enabled? connection :hornet-test-table)))
      (remove-table connection :hornet-test-table)))

  (deftest disable-table-test
    (th/create-test-table :hornet-test-table [:family]
      (fn []
        (testing "with enabled table"
          (is (false? (table-disabled? connection :hornet-test-table)))
          (disable-table connection :hornet-test-table)
          (is (true? (table-disabled? connection :hornet-test-table))))

        (testing "with non existent table"
          (is (thrown? org.apache.hadoop.hbase.TableNotFoundException
                       (table-disabled? connection :non-existent-test-table)))))))

  (deftest enable-table-test
    (th/create-test-table :hornet-test-table [:family]
      (fn []
        (testing "with disabled table"
          (disable-table connection :hornet-test-table)
          (is (false? (table-enabled? connection :hornet-test-table)))
          (enable-table connection :hornet-test-table)
          (is (true? (table-enabled? connection :hornet-test-table))))

        (testing "with non existent table"
          (is (thrown? org.apache.hadoop.hbase.TableNotFoundException
                       (table-enabled? connection :non-existent-test-table)))))))

  (deftest delete-table-test
    (testing "with non existent table"
      (is (thrown? org.apache.hadoop.hbase.TableNotFoundException
                   (disable-table connection :non-existent-test-table))))

    (testing "with enabled table"
      (create-table connection :hornet-test-table [:family])
      (is (thrown? org.apache.hadoop.hbase.TableNotDisabledException
                   (delete-table connection :hornet-test-table)))
      (is (true? (table-exists? connection :hornet-test-table)))
      ;; cleanup
      (disable-table connection :hornet-test-table)
      (delete-table connection :hornet-test-table))

    (testing "with disabled table"
      (create-table connection :hornet-test-table [:family])
      (disable-table connection :hornet-test-table)
      (delete-table connection :hornet-test-table)
      (is (false? (table-exists? connection :hornet-test-table)))))

  (deftest remove-table-test
    (testing "with non existent table"
      (is (thrown? org.apache.hadoop.hbase.TableNotFoundException
                   (disable-table connection :non-existent-test-table))))

    (testing "with enabled table"
      (create-table connection :hornet-test-table [:family])
      (remove-table connection :hornet-test-table)
      (is (false? (table-exists? connection :hornet-test-table))))

    (testing "with disabled table"
      (create-table connection :hornet-test-table [:family])
      (disable-table connection :hornet-test-table)
      (remove-table connection :hornet-test-table)
      (is (false? (table-exists? connection :hornet-test-table))))))

;; TODO truncate table with/without splits
