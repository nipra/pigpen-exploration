(ns pigpen-exploration.scratch
  (:require [pigpen.core :as pig]))

(def lokad-items-tsv
  "/Users/nprabhak/Projects/Clojure/pigpen-exploration/data/salescast-tsv-sample/Lokad_Items_modified.tsv")

(def input-tsv
  "/Users/nprabhak/Projects/Clojure/pigpen-exploration/data/input.tsv")

(defn my-data [data-file]
  (pig/load-tsv data-file))

(defn my-data* [data-file]
  (->>
   (pig/load-tsv data-file)
   (pig/map (fn [[a b c d e f g h i]]
              {:sum (+ (Integer/valueOf a) (Integer/valueOf i))
               :name b}))))

(defn my-data+ [data-file]
  (->>
   (pig/load-tsv data-file)
   (pig/map (fn [[a b c d e f g h i]]
              {:sum (+ (Integer/valueOf a) (Integer/valueOf i))
               :name b}))
   (pig/filter (fn [{:keys [sum]}]
                 (< sum 25)))))

;;;
(defn my-func [data]
  (->> data
       (pig/map (fn [[a b c d e f g h i]]
                  {:sum (+ (Integer/valueOf a) (Integer/valueOf i))
                   :name b}))
       (pig/filter (fn [{:keys [sum]}]
                     (< sum 25)))))

(defn my-query [input-file output-file]
  (->>
   (my-data input-file)
   (my-func)
   (pig/store-clj output-file)))


(comment
  (spit input-tsv "1\t2\ta\n4\t5\tb\n1\t2\tc\n4\t5\td"))

(comment
  (pig/dump
   (my-data lokad-items-tsv))

  (pig/dump (my-data* lokad-items-tsv))
  (pig/dump (my-data+ lokad-items-tsv)))

(comment
  (pig/dump (my-func (my-data lokad-items-tsv)))
  (pig/write-script "my-script.pig" (my-query lokad-items-tsv "output"))
  "(nprabhak@nprabhak-mn ~/Projects/Clojure/pigpen-exploration)$ lein uberjar
   (nprabhak@nprabhak-mn ~/Projects/Clojure/pigpen-exploration)$ cp target/pigpen-exploration-0.1.0-SNAPSHOT-standalone.jar pigpen.jar
   (nprabhak@nprabhak-mn ~/Projects/Clojure/pigpen-exploration)$ pig -x local -f my-script.pig
   (nprabhak@nprabhak-mn ~/Projects/Clojure/pigpen-exploration)$ less output/part-m-00000")

;;; (nprabhak@nprabhak-mn ~/Projects/Clojure/pigpen-exploration)$ hadoop fs -put data/salescast-tsv-sample/Lokad_Items_modified.tsv /data/

(def lokad-items-tsv-hdfs "/data/Lokad_Items_modified.tsv")

(comment
  (pig/write-script "my-script2.pig" (my-query lokad-items-tsv-hdfs "/output-lokad"))
  "(nprabhak@nprabhak-mn ~/Projects/Clojure/pigpen-exploration)$ lein uberjar
   (nprabhak@nprabhak-mn ~/Projects/Clojure/pigpen-exploration)$ cp target/pigpen-exploration-0.1.0-SNAPSHOT-standalone.jar pigpen.jar
   (nprabhak@nprabhak-mn ~/Projects/Clojure/pigpen-exploration)$ pig -f my-script.pig
   (nprabhak@nprabhak-mn ~/Projects/Clojure/pigpen-exploration)$ hadoop fs -cat /output-lokad/part-m-00000 | less")
