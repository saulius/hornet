(ns hornet.requests-test
  (:refer-clojure :rename {get map-get})
  (:require [clojure.test :refer :all]
            [hornet.conversions :refer [to-bytes]]
            [hornet.requests :refer :all]))

(deftest put-test-sets-row-key
  (testing "sets row key"
    (let [request (put "rk" {})]
      (is (= "rk" (row request))))))

(deftest put-test-sets-columns-and-values
  (testing "sets columns and values"
    (let [cols-and-vals {"fam1" {"col1" "val1"
                                 "col2" "val2"}
                         "fam2" {"col3" "val3"}}
          request (put "rk" cols-and-vals)]
      (is (= true (has? request "fam1" "col1" "val1")))
      (is (= true (has? request "fam1" "col2" "val2")))
      (is (= true (has? request "fam2" "col3" "val3"))))))

(deftest put-test-sets-id
  (testing "sets id"
    (let [request (put "rk" {"f" {"c" "v"}} {:id "42"})]
      (is (= "42" (id request))))))

(deftest put-test-sets-ttl
  (testing "sets ttl"
    (let [request (put "rk" {"f" {"c" "v"}} {:ttl 42})]
      (is (= 42 (ttl request))))))

(deftest put-test-sets-durability
  (testing "sets durability"
    (let [request (put "rk" {"f" {"c" "v"}} {:durability :skip-wal})]
      (is (= :skip-wal (durability request))))))

(deftest put-test-sets-custom-timestamp
  (testing "sets custom timestamp for cells"
    (let [request (put "rk" {"f" {"c" {123 "v"}
                                  "c2" {999 "val"}}})]
      (is (has? request "f" "c" 123 "v"))
      (is (has? request "f" "c2" 999 "val")))))

(deftest get-test-sets-row-key
  (testing "sets row key"
    (let [request (get "rk")]
      (is (= "rk" (row request))))))

(deftest get-test-sets-min-max-timestamp
  (testing "sets min and max timestamps"
    (let [request (get "rk" {:min-timestamp 123 :max-timestamp 321})]
      (is (= 123 (min-timestamp request)))
      (is (= 321 (max-timestamp request))))))

(deftest get-test-sets-single-timestamp
  (testing "sets single timestamp"
    (let [request (get "rk" {:timestamp 123})]
      (is (= 123 (min-timestamp request)))
      (is (= 124 (max-timestamp request))))))

(deftest get-test-sets-max-versions
  (testing "sets max versions to fetch"
    (let [request (get "rk" {:max-versions 2})]
      (is (= 2 (versions request))))))

(deftest get-test-sets-fetch-all-versions
  (testing "fetch all available versions"
    (let [request (get "rk" {:all-versions true})]
      (is (= Integer/MAX_VALUE (versions request))))))

(deftest get-test-sets-cache-blocks
  (testing "sets cache blocks"
    (let [request (get "rk" {:cache-blocks false})]
      (is (= false (cache-blocks? request))))))

(deftest get-test-sets-store-limit
  (testing "sets store limit"
    (let [request (get "rk" {:store-limit 33})]
      (is (= 33 (store-limit request))))))

(deftest get-test-sets-store-offset
  (testing "sets store offset"
    (let [request (get "rk" {:store-offset 11})]
      (is (= 11 (store-offset request))))))

(deftest get-test-sets-columns
  (testing "sets columns"
    (let [c {"f1" ["col1" "col2"]
             "f2" ["col2"]}
          request (get "rk" {:columns c})]
      (is (= c (columns request))))))

(deftest get-test-sets-families
  (testing "sets families"
    (let [request (get "rk" {:families ["f1" "f2"]})]
      (is (= ["f1" "f2"] (families request))))))

(deftest delete-test-sets-row-key
  (testing "sets row key"
    (let [request (delete "rk" {})]
      (is (= "rk" (row request))))))

(deftest delete-test-sets-id
  (testing "sets id"
    (let [request (delete "rk" {:id "42"})]
      (is (= "42" (id request))))))

(deftest delete-test-sets-ttl
  (testing "sets ttl"
    (let [request (delete "rk" {:ttl 42})]
      (is (= 42 (ttl request))))))

(deftest delete-test-sets-durability
  (testing "sets durability"
    (let [request (delete "rk" {:durability :skip-wal})]
      (is (= :skip-wal (durability request))))))

(deftest increment-test-sets-min-max-timestamp
  (testing "sets min and max timestamps"
    (let [request (increment "rk"
                             {:family {:column 1}}
                             {:min-timestamp 123 :max-timestamp 321})]
      (is (= 123 (:min-timestamp (time-range request))))
      (is (= 321 (:max-timestamp (time-range request)))))))

(deftest scan-test-sets-start-row
  (testing "sets start row"
    (let [request (scan {:start-row "abc"})]
      (is (= "abc" (start-row request))))))

(deftest scan-test-sets-stop-row
  (testing "sets stop row"
    (let [request (scan {:stop-row "xyz"})]
      (is (= "xyz" (stop-row request))))))

(deftest scan-test-sets-max-versions
  (testing "sets max versions"
    (let [request (scan {:max-versions 8})]
      (is (= 8 (max-versions request))))))

(deftest scan-test-sets-batch-size
  (testing "sets batch size"
    (let [request (scan {:batch-size 9999})]
      (is (= 9999 (batch-size request))))))

(deftest scan-test-sets-store-limit
  (testing "sets store limit"
    (let [request (scan {:store-limit 999})]
      (is (= 999 (store-limit request))))))

(deftest scan-test-sets-store-offset
  (testing "sets store limit"
    (let [request (scan {:store-offset 123})]
      (is (= 123 (store-offset request))))))

(deftest scan-test-sets-caching
  (testing "sets caching"
    (let [request (scan {:caching 1010})]
      (is (= 1010 (caching? request))))))

(deftest scan-test-sets-cache-blocks
  (testing "sets cache blocks"
    (let [request (scan {:cache-blocks false})]
      (is (= false (cache-blocks? request))))))

(deftest scan-test-sets-reversed
  (testing "sets reversed"
    (let [request (scan {:reversed true})]
      (is (= true (reversed? request))))))

(deftest scan-test-sets-small
  (testing "sets small"
    (let [request (scan {:small true})]
      (is (= true (small? request))))))

(deftest scan-test-sets-load-column-families-on-demand
  (testing "sets load column failies on demand"
    (let [request (scan {:load-column-families-on-demand true})]
      (is (= true (load-column-families-on-demand? request))))))

(deftest scan-test-sets-max-result-size
  (testing "sets max result size"
    (let [request (scan {:max-result-size 333})]
      (is (= 333 (max-result-size request))))))

(deftest scan-test-sets-time-range
  (testing "sets time range (min timestamp + max timestamp)"
    (let [request (scan {:min-timestamp 123 :max-timestamp 321})]
      (is (= 123 (min-timestamp request)))
      (is (= 321 (max-timestamp request))))))
