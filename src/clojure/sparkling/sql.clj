(ns sparkling.sql
  (:import [org.apache.spark.sql SQLContext Row]))



(defn sql-context [spark-context]
  (SQLContext. spark-context))


(defn sql [sql-context query]
  (.sql sql-context query))

(defn read-json [sql-context path]
  (-> sql-context
      (.read)
      (.json path)))

(defn register-temp-table [df name]
  (.registerTempTable df name))
