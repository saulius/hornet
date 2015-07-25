(ns hornet.utils-test
  (:require [clojure.test :refer :all]
            [hornet.utils :refer :all]))

(deftest flatten-map-test
  (testing "with keys as keywords"
    (is (= [[[:a :b :c] :d]] (flatten-map {:a {:b {:c :d}}}))))

  (testing "with keys as strings"
    (is (= [[["a" "b" "c"] "d"]] (flatten-map {"a" {"b" {"c" "d"}}}))))

  (testing "with multiple paths, various nesting levels"
    (is (= [[[:a :b :c] :d]
            [[:a :b :e] :f]
            [[:a :g] :h]] (flatten-map {:a {:b {:c :d
                                                :e :f}
                                            :g :h}})))))
