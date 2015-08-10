(ns hornet.client-test
  (:refer-clojure :rename {get map-get})
  (:require [clojure.test :refer :all]
            [hornet.test-helpers :as th]
            [hornet.conversions :refer [to-clojure]]
            [hornet.client :refer :all])
  (:import [org.apache.hadoop.hbase.util Bytes]))

(use-fixtures :once
  (fn [all-tests]
    (th/create-test-table :_hornet-table {:hf {:max-versions 3}})
    (th/create-test-table :_hornet-multi-family-table [:f1 :f2 :f3])
    (all-tests)))

(use-fixtures :each
  (fn [test]
    (th/truncate-tables [:_hornet-table :_hornet-multi-family-table])
    (test)))

(let [connection (th/make-test-connection)
      table-name :_hornet-table
      multi-family-table-name :_hornet-multi-family-table]
  (deftest put-test-args-as-keywords
    (testing "simple put with family and column as keywords"
      (put connection table-name "rowkey" {:hf {:b "b"}})
      (is (not-empty (get connection table-name "rowkey")))))

  (deftest put-test-args-as-strings
    (testing "simple put with family and column as strings"
      (put connection table-name "rowkey" {"hf" {"b" "b"}})
      (is (not-empty (get connection table-name "rowkey")))))

  (deftest put-test-custom-timestamp
    (testing "put with custom timestamp"
      (let [timestamp 1437928014]
        (put connection table-name "rowkey" {:hf {:b {timestamp "b"}}})
        (is (not-empty (get connection table-name "rowkey" {:timestamp timestamp})))
        (is (empty (get connection table-name "rowkey" {:timestamp (inc timestamp)}))))))

  (deftest put-test-multiple-columns
    (testing "put with multiple columns"
      (put connection table-name "rowkey" {:hf {:b "b"
                                                :c "d"}})
      (let [result (get connection table-name "rowkey")]
        (is (not-empty result))
        (is (= 2 (count result)))
        (is (= #{"b" "c"} (set (map #(-> % :qualifier to-clojure) result)))))))

  (deftest put-test-multiple-columns-families
    (testing "put with multiple column families"
      (put connection multi-family-table-name "rowkey" {:f1 {:b "b"}
                                                        :f2 {:c "d"}})
      (let [result (get connection multi-family-table-name "rowkey")]
        (is (not-empty result))
        (is (= 2 (count result)))
        (is (= #{'("f1" "b")
                 '("f2" "c")} (set (map #(list (-> % :family to-clojure)
                                               (-> % :qualifier to-clojure)) result)))))))

  (deftest get-test-by-rowkey
    (testing "get by rowkey"
      (put connection table-name "rowkey" {:hf {:b "b"}})
      (is (not-empty (get connection table-name "rowkey")))))

  (deftest get-test-by-rowkey-multiple-versions
    (testing "get by rowkey with several versions returns all versions"
      (put connection table-name "rowkey" {:hf {:f {123 "b"
                                                    999 "c"}}})
      (let [result (get connection table-name "rowkey")
            res (first result)]
        (is (= 1 (count result)))
        (is (= "c" (to-clojure (:value res))))
        (is (= 999 (to-clojure (:timestamp res)))))))

  (deftest get-test-with-timestamp
    (testing "get with timestamp"
      (put connection table-name "rowkey" {:hf {:b {123 "b"
                                                    435 "c"}}})
      (let [result (get connection table-name "rowkey" {:timestamp 435})
            res (first result)]
        (is (not-empty result))
        (is (= 1 (count result)))
        (is (= "c" (to-clojure (:value res))))
        (is (= 435 (to-clojure (:timestamp res)))))))

  (deftest get-test-with-timestamp-range
    (testing "get with timestamp range"
      (put connection table-name "rowkey" {:hf {:b {222 "b"
                                                    444 "c"}}})
      (let [result (get connection table-name "rowkey" {:min-timestamp 111
                                                        :max-timestamp 333})
            res (first result)]
        (is (not-empty result))
        (is (= 1 (count result)))
        (is (= "b" (to-clojure (:value res))))
        (is (= 222 (to-clojure (:timestamp res)))))))

  (deftest get-test-with-max-versions-to-fetch
    (testing "get max versions"
      (put connection table-name "rowkey" {:hf {:a {123 "v1"}}})
      (put connection table-name "rowkey" {:hf {:a {456 "v2"}}})
      (let [result (get connection table-name "rowkey" {:max-versions 2})
            res (first result)]
        (is (not-empty result))
        (is (= 2 (count result)))
        (is (= "v2" (to-clojure (:value res))))
        (is (= 456 (to-clojure (:timestamp res)))))))

  (deftest get-test-with-all-versions-to-fetch
    (testing "get all versions"
      (put connection table-name "rowkey" {:hf {:a {123 "v1"}}})
      (put connection table-name "rowkey" {:hf {:a {456 "v2"}}})
      (put connection table-name "rowkey" {:hf {:a {888 "v3"}}})
      (let [result (get connection table-name "rowkey" {:all-versions true})]
        (is (not-empty result))
        (is (= 3 (count result)))
        (is (= #{"v1" "v2" "v3"} (set (map #(-> % :value to-clojure) result)))))))

  (deftest get-test-with-specific-families
    (testing "get specific families"
      (put connection multi-family-table-name "rowkey" {:f1 {:a {123 "v1"}}})
      (put connection multi-family-table-name "rowkey" {:f2 {:a {456 "v2"}}})
      (let [result (get connection multi-family-table-name "rowkey" {:families [:f2]})]
        (is (= 1 (count result)))
        (is (= "f2" (-> result first :family to-clojure)))
        (is (= "v2" (-> result first :value to-clojure))))))

  (deftest get-test-with-specific-families-and-columns
    (testing "get specific families and columns"
      (put connection table-name "rowkey" {:hf {:a {123 "v1"}
                                                :b {444 "v2"}}})
      (let [result (get connection table-name "rowkey" {:columns {:hf [:b]}})]
        (is (= 1 (count result)))
        (is (= "hf" (-> result first :family to-clojure)))
        (is (= "b" (-> result first :qualifier to-clojure)))
        (is (= "v2" (-> result first :value to-clojure))))))

  (deftest delete-test-rowkey-keyword
    (testing "simple delete with rowkey as keyword"
      (put connection table-name "rowkey" {:hf {:b "b"}})
      (is (not-empty (get connection table-name "rowkey")))
      (delete connection table-name "rowkey")
      (is (empty (get connection table-name "rowkey")))))

  (deftest delete-test-by-family
    (testing "delete specific family only"
      (put connection multi-family-table-name "rowkey" {:f1  {:a "a"}
                                                        :f2  {:b "b"}
                                                        :f3  {:c "c"}})
      (is (not-empty (get connection multi-family-table-name "rowkey" {:families [:f1]})))
      (is (not-empty (get connection multi-family-table-name "rowkey" {:families [:f2]})))
      (is (not-empty (get connection multi-family-table-name "rowkey" {:families [:f3]})))
      (delete connection multi-family-table-name "rowkey" {:family :f2})
      (is (not-empty (get connection multi-family-table-name "rowkey" {:families [:f1]})))
      (is (empty (get connection multi-family-table-name "rowkey" {:families [:f2]})))
      (is (not-empty (get connection multi-family-table-name "rowkey" {:families [:f3]})))))

  (deftest delete-test-by-families
    (testing "delete specific families only"
      (put connection multi-family-table-name "rowkey" {:f1  {:a "a"}
                                                        :f2  {:b "b"}
                                                        :f3  {:c "c"}})
      (is (not-empty (get connection multi-family-table-name "rowkey" {:families [:f1]})))
      (is (not-empty (get connection multi-family-table-name "rowkey" {:families [:f2]})))
      (is (not-empty (get connection multi-family-table-name "rowkey" {:families [:f3]})))
      (delete connection multi-family-table-name "rowkey" {:families [:f2 :f3]})
      (is (not-empty (get connection multi-family-table-name "rowkey" {:families [:f1]})))
      (is (empty (get connection multi-family-table-name "rowkey" {:families [:f2]})))
      (is (empty (get connection multi-family-table-name "rowkey" {:families [:f3]})))))

  (deftest delete-test-by-columns
    (testing "delete specific columns only"
      (put connection multi-family-table-name "rowkey" {:f1  {:a "a"
                                                              :b "b"
                                                              :c "c"}})
      (is (not-empty (get connection multi-family-table-name "rowkey" {:columns {:f1 [:a :b :c]}})))
      (delete connection multi-family-table-name "rowkey" {:columns {:f1 [:a :b]}})
      (is (empty (get connection multi-family-table-name "rowkey" {:columns {:f1 [:a]}})))
      (is (empty (get connection multi-family-table-name "rowkey" {:columns {:f1 [:b]}})))
      (is (not-empty (get connection multi-family-table-name "rowkey" {:columns {:f1
                                                                                 [:c]}})))))

  (deftest increment-test-single-column
    (testing "increments single column"
      (put connection table-name "rowkey" {:hf {:f 10}})
      (increment connection table-name "rowkey" {:hf {:f 1}})
      (let [result (get connection table-name "rowkey")
            res (first result)]
        (is (= 11 (Bytes/toLong (:value res)))))))

  (deftest increment-test-increment-by-not-one
    (testing "increments single column by value other than 1"
      (put connection table-name "rowkey" {:hf {:f 10}})
      (increment connection table-name "rowkey" {:hf {:f 5}})
      (let [result (get connection table-name "rowkey")
            res (first result)]
        (is (= 15 (Bytes/toLong (:value res)))))))

  (deftest increment-test-increment-by-negative-num
    (testing "increments single column by a negative value (subtracts)"
      (put connection table-name "rowkey" {:hf {:f 10}})
      (increment connection table-name "rowkey" {:hf {:f -5}})
      (let [result (get connection table-name "rowkey")
            res (first result)]
        (is (= 5 (Bytes/toLong (:value res)))))))

  (deftest increment-test-multiple-columns
    (testing "increments single column by a negative value (subtracts)"
      (put connection table-name "rowkey" {:hf {:f1 10
                                                :f2 15}})
      (increment connection table-name "rowkey" {:hf {:f1 5
                                                      :f2 -5}})
      (let [result (get connection table-name "rowkey")
            [res-f1 res-f2] result]
        (is (= "f1" (to-clojure (:qualifier res-f1))))
        ;; TODO need schemas Bytes/toLong is a bad idea
        (is (= 15 (Bytes/toLong (:value res-f1))))
        (is (= "f2" (to-clojure (:qualifier res-f2))))
        (is (= 10 (Bytes/toLong (:value res-f2)))))))

  (deftest increment-test-multiple-column-families
    (testing "increments columns in multiple families"
      (put connection multi-family-table-name "rowkey" {:f1  {:a 1}
                                                        :f2  {:b 10}
                                                        :f3  {:c 30}})
      (increment connection multi-family-table-name "rowkey" {:f1 {:a 5}
                                                              :f2 {:b 5}
                                                              :f3 {:c -5}})
      (let [result (get connection multi-family-table-name "rowkey")
            [res-f1 res-f2 res-f3] result]
        (is (= "f1" (to-clojure (:family res-f1))))
        (is (= "a" (to-clojure (:qualifier res-f1))))
        (is (= 6 (Bytes/toLong (:value res-f1))))

        (is (= "f2" (to-clojure (:family res-f2))))
        (is (= "b" (to-clojure (:qualifier res-f2))))
        (is (= 15 (Bytes/toLong (:value res-f2))))

        (is (= "f3" (to-clojure (:family res-f3))))
        (is (= "c" (to-clojure (:qualifier res-f3))))
        (is (= 25 (Bytes/toLong (:value res-f3)))))))

  (deftest scan-test-basic
    (testing "basic scanning returns results"
      (put connection table-name "rowkey1" {:hf {:col1 "val1"}})
      (put connection table-name "rowkey2" {:hf {:col2 "val2"}})
      (let [result (flatten (scan connection table-name))]
        (is (= ["col1" "col2"] (map #(-> % :qualifier to-clojure) result)))
        (is (= ["val1" "val2"] (map #(-> % :value to-clojure) result))))))

  (deftest scan-test-start-stop-keys
    (testing "scanning with start and stop keys set"
      (put connection table-name "rowkey1" {:hf {:col1 "val1"}})
      (put connection table-name "rowkey2" {:hf {:col2 "val2"}})
      (put connection table-name "rowkey3" {:hf {:col3 "val3"}})
      (let [result (flatten (scan connection table-name {:start-row "rowkey2"
                                                         :stop-row  "rowkey4"}))]
        (is (= ["col2" "col3"] (map #(-> % :qualifier to-clojure) result)))
        (is (= ["val2" "val3"] (map #(-> % :value to-clojure) result))))))

  (deftest scan-test-min-max-timestamp
    (testing "scanning with min+max timestamps"
      (put connection table-name "rowkey1" {:hf {:col1 {1 "val1"}}})
      (put connection table-name "rowkey2" {:hf {:col2 {2 "val2"}}})
      (put connection table-name "rowkey3" {:hf {:col3 {5 "val3"}}})
      (let [result (flatten (scan connection table-name {:min-timestamp 5
                                                         :max-timestamp 8}))]
        (is (= ["col3"] (map #(-> % :qualifier to-clojure) result)))
        (is (= ["val3"] (map #(-> % :value to-clojure) result))))))

  (deftest scan-test-reversed
    (testing "scanning in reverse order"
      (put connection table-name "rowkey1" {:hf {:col1 {1 "val1"}}})
      (put connection table-name "rowkey2" {:hf {:col2 {2 "val2"}}})
      (put connection table-name "rowkey3" {:hf {:col3 {5 "val3"}}})
      (let [result (flatten (scan connection table-name {:reversed true}))]
        (is (= ["col3" "col2" "col1"] (map #(-> % :qualifier to-clojure) result)))
        (is (= ["val3" "val2" "val1"] (map #(-> % :value to-clojure) result)))))))
