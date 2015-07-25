(ns hornet.utils)

(defn flatten-map
 ([kvs] (flatten-map [] kvs))
 ([pk kvs]
   (mapcat (fn [[k v]]
             (if (map? v)
               ;; TODO make this tail recursive, extract to helper fn
               (flatten-map (conj pk k) v)
               [[(conj pk k) v]])) kvs)))
