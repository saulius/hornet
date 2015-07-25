(ns hornet.client-test
  (:require [clojure.test :refer :all]
            [hornet.test-helpers :as th]
            [hornet.conversions :refer [to-clojure]]
            [hornet.client :refer :all])
  (:import [org.apache.hadoop.hbase.util Bytes]))

(use-fixtures :once
  (fn [all-tests]
    (th/create-test-table :_hornet-table {:hornet-family {:max-versions 3}})
    (th/create-test-table :multi-family-table [:f1 :f2 :f3])
    (all-tests)))

(use-fixtures :each
  (fn [test]
    (th/truncate-tables [:_hornet-table :multi-family-table])
    (test)))

(let [connection (th/make-test-connection)
      multi-family-table-name :multi-family-table]
  (deftest put-test-args-as-keywords
    (testing "simple put with family and column as keywords"
      (put connection :_hornet-table "rowkey" {:hornet-family {:b "b"}})
      (is (not-empty (get connection :_hornet-table "rowkey")))))

  (deftest put-test-args-as-strings
    (testing "simple put with family and column as strings"
      (put connection :_hornet-table "rowkey" {"hornet-family" {"b" "b"}})
      (is (not-empty (get connection :_hornet-table "rowkey")))))

  (deftest put-test-custom-timestamp
    (testing "put with custom timestamp"
      (let [timestamp 1437928014]
        (put connection :_hornet-table "rowkey" {:hornet-family {:b {timestamp "b"}}})
        (is (not-empty (get connection :_hornet-table "rowkey" {:timestamp timestamp})))
        (is (empty (get connection :_hornet-table "rowkey" {:timestamp (inc timestamp)}))))))

  (deftest put-test-multiple-columns
    (testing "put with multiple columns"
      (put connection :_hornet-table "rowkey" {:hornet-family {:b "b"
                                                              :c "d"}})
      (let [result (get connection :_hornet-table "rowkey")]
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
      (put connection :_hornet-table "rowkey" {:hornet-family {:b "b"}})
      (is (not-empty (get connection :_hornet-table "rowkey")))))

  (deftest get-test-by-rowkey-multiple-versions
    (testing "get by rowkey with several versions returns all versions"
      (put connection :_hornet-table "rowkey" {:hornet-family {:f {123 "b"
                                                                  999 "c"}}})
      (let [result (get connection :_hornet-table "rowkey")
            res (first result)]
        (is (= 1 (count result)))
        (is (= "c" (to-clojure (:value res))))
        (is (= 999 (to-clojure (:timestamp res)))))))

  (deftest get-test-with-timestamp
    (testing "get with timestamp"
      (put connection :_hornet-table "rowkey" {:hornet-family {:b {123 "b"
                                                                  435 "c"}}})
      (let [result (get connection :_hornet-table "rowkey" {:timestamp 435})
            res (first result)]
        (is (not-empty result))
        (is (= 1 (count result)))
        (is (= "c" (to-clojure (:value res))))
        (is (= 435 (to-clojure (:timestamp res)))))))

  (deftest get-test-with-timestamp-range
    (testing "get with timestamp range"
      (put connection :_hornet-table "rowkey" {:hornet-family {:b {222 "b"
                                                                  444 "c"}}})
      (let [result (get connection :_hornet-table "rowkey" {:min-timestamp 111
                                                           :max-timestamp 333})
            res (first result)]
        (is (not-empty result))
        (is (= 1 (count result)))
        (is (= "b" (to-clojure (:value res))))
        (is (= 222 (to-clojure (:timestamp res)))))))

  (deftest get-test-with-max-versions-to-fetch
    (testing "get max versions"
      (put connection :_hornet-table "rowkey" {:hornet-family {:a {123 "v1"}}})
      (put connection :_hornet-table "rowkey" {:hornet-family {:a {456 "v2"}}})
      (let [result (get connection :_hornet-table "rowkey" {:max-versions 2})
            res (first result)]
        (is (not-empty result))
        (is (= 2 (count result)))
        (is (= "v2" (to-clojure (:value res))))
        (is (= 456 (to-clojure (:timestamp res)))))))

  (deftest get-test-with-all-versions-to-fetch
    (testing "get all versions"
      (put connection :_hornet-table "rowkey" {:hornet-family {:a {123 "v1"}}})
      (put connection :_hornet-table "rowkey" {:hornet-family {:a {456 "v2"}}})
      (put connection :_hornet-table "rowkey" {:hornet-family {:a {888 "v3"}}})
      (let [result (get connection :_hornet-table "rowkey" {:all-versions true})]
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
      (put connection :_hornet-table "rowkey" {:hornet-family {:a {123 "v1"}
                                                              :b {444 "v2"}}})
      (let [result (get connection :_hornet-table "rowkey" {:columns {:hornet-family [:b]}})]
        (is (= 1 (count result)))
        (is (= "hornet-family" (-> result first :family to-clojure)))
        (is (= "b" (-> result first :qualifier to-clojure)))
        (is (= "v2" (-> result first :value to-clojure))))))

  (deftest delete-test-rowkey-keyword
    (testing "simple delete with rowkey as keyword"
      (put connection :_hornet-table "rowkey" {:hornet-family {:b "b"}})
      (is (not-empty (get connection :_hornet-table "rowkey")))
      (delete connection :_hornet-table "rowkey")
      (is (empty (get connection :_hornet-table "rowkey")))))

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
    (testing "delete specific column sonly"
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
      (put connection :_hornet-table "rowkey" {:hornet-family {:f 10}})
      (increment connection :_hornet-table "rowkey" {:hornet-family {:f 1}})
      (let [result (get connection :_hornet-table "rowkey")
            res (first result)]
        (is (= 11 (Bytes/toLong (:value res)))))))

  (deftest increment-test-increment-by-not-one
    (testing "increments single column by value other than 1"
      (put connection :_hornet-table "rowkey" {:hornet-family {:f 10}})
      (increment connection :_hornet-table "rowkey" {:hornet-family {:f 5}})
      (let [result (get connection :_hornet-table "rowkey")
            res (first result)]
        (is (= 15 (Bytes/toLong (:value res)))))))

  (deftest increment-test-increment-by-negative-num
    (testing "increments single column by a negative value (subtracts)"
      (put connection :_hornet-table "rowkey" {:hornet-family {:f 10}})
      (increment connection :_hornet-table "rowkey" {:hornet-family {:f -5}})
      (let [result (get connection :_hornet-table "rowkey")
            res (first result)]
        (is (= 5 (Bytes/toLong (:value res)))))))

  (deftest increment-test-multiple-columns
    (testing "increments single column by a negative value (subtracts)"
      (put connection :_hornet-table "rowkey" {:hornet-family {:f1 10
                                                              :f2 15}})
      (increment connection :_hornet-table "rowkey" {:hornet-family {:f1 5
                                                                    :f2 -5}})
      (let [result (get connection :_hornet-table "rowkey")
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
        (is (= 25 (Bytes/toLong (:value res-f3))))))))
