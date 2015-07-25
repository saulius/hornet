(ns hornet.filter
  (:require [hornet.conversions :refer [to-bytes]])
  (:import [org.apache.hadoop.hbase.filter
            ;; Base classes
            Filter
            CompareFilter
            CompareFilter$CompareOp
            ;; Comparators
            BitComparator
            BitComparator$BitwiseOp
            BinaryComparator
            BinaryPrefixComparator
            LongComparator
            NullComparator
            RegexStringComparator
            SubstringComparator
            ;; Filters
            ColumnCountGetFilter
            ColumnPaginationFilter
            ColumnPrefixFilter
            ColumnRangeFilter
            DependentColumnFilter
            FamilyFilter
            FilterList
            FilterList$Operator
            FirstKeyOnlyFilter
            FirstKeyValueMatchingQualifiersFilter
            FuzzyRowFilter
            InclusiveStopFilter
            KeyOnlyFilter
            MultipleColumnPrefixFilter
            PageFilter
            PrefixFilter
            QualifierFilter
            RandomRowFilter
            RowFilter
            SingleColumnValueExcludeFilter
            SingleColumnValueFilter
            SkipFilter
            TimestampsFilter
            ValueFilter
            WhileMatchFilter]))

(defn and
  [filters]
  (let [filter-list (FilterList. FilterList$Operator/MUST_PASS_ALL)]
    (doseq [filter filters]
      (.addFilter filter-list filter))
    filter-list))

(defn or
  [filters]
  (let [filter-list (FilterList. FilterList$Operator/MUST_PASS_ONE)]
    (doseq [filter filters]
      (.addFilter filter-list filter))
    filter-list))

(defn operator
  [operator]
  (condp = operator
    :=    CompareFilter$CompareOp/EQUAL
    :>    CompareFilter$CompareOp/GREATER
    :>=   CompareFilter$CompareOp/GREATER_OR_EQUAL
    :<=   CompareFilter$CompareOp/LESS_OR_EQUAL
    :<    CompareFilter$CompareOp/LESS
    :not= CompareFilter$CompareOp/NOT_EQUAL
    :noop CompareFilter$CompareOp/NO_OP))

(defn bit-operator
  [operator]
  (condp = operator
    :and BitComparator$BitwiseOp/AND
    :or  BitComparator$BitwiseOp/OR
    :xor BitComparator$BitwiseOp/XOR))

(defmulti apply-criteria (fn [x] (-> x keys first)))
(defmethod apply-criteria :or [criteria] (or (:or criteria)))
(defmethod apply-criteria :and [criteria] (and (:and criteria)))
(defmethod apply-criteria :bit [criteria]
  (let [conditions (:bit criteria)]
    (BitComparator. (to-bytes (:value conditions)) (bit-operator (:op conditions)))))
(defmethod apply-criteria :column-prefix [criteria]
  (ColumnPrefixFilter. (to-bytes (get-in criteria [:column-prefix :prefix]))))
(defmethod apply-criteria :multiple-column-prefix [criteria]
  (MultipleColumnPrefixFilter. (into-array (map to-bytes (get-in criteria [:multiple-column-prefix :prefixes])))))
(defmethod apply-criteria :op [criteria] (update-in criteria [:op] operator))
(defmethod apply-criteria :regex [criteria]
  (RegexStringComparator. (:regex criteria)))
(defmethod apply-criteria :substring [criteria]
  (SubstringComparator. (:substring criteria)))
(defmethod apply-criteria :binary-prefix [criteria]
  (BinaryPrefixComparator. (to-bytes (:binary-prefix criteria))))
(defmethod apply-criteria :key-only [criteria]
  (KeyOnlyFilter.))
(defmethod apply-criteria :inclusive-stop [criteria]
  (let [conditions (:inclusive-stop criteria)]
    (InclusiveStopFilter. (to-bytes (:stop-row-key conditions)))))
(defmethod apply-criteria :page [criteria]
  (let [conditions (:page criteria)]
    (PageFilter. (:size conditions))))
(defmethod apply-criteria :timestamps [criteria]
  (TimestampsFilter. (:timestamps criteria)))
(defmethod apply-criteria :binary [criteria]
  (BinaryComparator. (to-bytes (:binary criteria))))
(defmethod apply-criteria :family [criteria]
  (let [conditions (:family criteria)]
    (FamilyFilter. (:op conditions) (:comparator conditions))))
(defmethod apply-criteria :value [criteria]
  (let [conditions (:value criteria)]
    (ValueFilter. (:op conditions) (:comparator conditions))))
(defmethod apply-criteria :column-count-get [criteria]
  (ColumnCountGetFilter. (:column-count-get criteria)))
(defmethod apply-criteria :column-pagination [criteria]
  (let [conditions (:column-pagination criteria)]
    (ColumnPaginationFilter. ^int (:limit conditions) ^int (:offset conditions))))
(defmethod apply-criteria :qualifier [criteria]
  (let [conditions (:qualifier criteria)]
    (QualifierFilter. (:op conditions) (:comparator conditions))))
(defmethod apply-criteria :row [criteria]
  (let [conditions (:row criteria)]
    (RowFilter. (:op conditions) (:comparator conditions))))
(defmethod apply-criteria :first-key-value-matching-qualifiers [criteria]
  (FirstKeyValueMatchingQualifiersFilter.
   (let [conditions (:first-key-value-matching-qualifiers criteria)]
     (set (map to-bytes (:qualifiers conditions))))))

(defmethod apply-criteria :skip [criteria]
  (SkipFilter. (:skip criteria)))
(defmethod apply-criteria :column-range [criteria]
  (let [conditions (:column-range criteria)]
    (ColumnRangeFilter. (to-bytes (:min-column conditions))
                        (:min-inclusive conditions)
                        (to-bytes (:max-column conditions))
                        (:max-inclusive conditions))))
(defmethod apply-criteria :first-key-only [criteria]
  (FirstKeyOnlyFilter.))
(defmethod apply-criteria :single-column-value [criteria]
  (let [conditions (:single-column-value criteria)]
    (SingleColumnValueFilter. (to-bytes (:family conditions))
                                (to-bytes (:column conditions))
                                (:op conditions)
                                (:comparator conditions))))

(defmethod apply-criteria :default [criteria]
  (prn "Using default matcher for " criteria)
  criteria)

(defn filter [criteria]
  (clojure.walk/postwalk
   (fn [condition]
     (if (map? condition)
       (apply-criteria condition)
       condition))
   criteria))

(def q {:or [{:column-prefix {:prefix "abc"}}
             {:and [{:family {:op :=
                              :comparator {:binary "value"}}}
                    {:skip {:family {:op :=
                                     :comparator {:binary "value"}}}}
                    {:qualifier {:op :<
                                 :comparator {:substring "lol"}}}
                    {:value {:op :=
                             :comparator {:binary "wat"}}}
                    ;; {:qualifier {:op :<
                    ;;              :comparator {:bit {:value "lol"
                    ;;                                 :op :xor}}}}
                    {:first-key-only true}
                    {:column-count-get 6}
                    {:inclusive-stop {:stop-row-key "wat"}}
                    {:key-only true}
                    {:page {:size 10}}
                    {:row {:op :<
                           :comparator {:substring "lol"}}}
                    {:column-pagination {:limit 10
                                         :offset 10}}
                    {:first-key-value-matching-qualifiers {:qualifiers #{"a" "b" "c"}}}
                    {:multiple-column-prefix {:prefixes ["abc" "def" "lol"]}}
                    {:column-range {:min-column "lolwat"
                                    :min-inclusive true
                                    :max-column "yo"
                                    :max-inclusive true}}
                    {:timestamps [123 323]}
                    {:single-column-value {:op :<=
                                           :family "family"
                                           :column "column"
                                           :comparator {:binary "value"}}}]}]})

(filter q)
