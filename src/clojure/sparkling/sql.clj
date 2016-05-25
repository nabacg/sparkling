(ns sparkling.sql
  (:require [sparkling.conf :as conf]
            [sparkling.core :as spark]
            [sparkling.api :as api]
            [sparkling.core :as spark]
            [sparkling.sql :as sql])
  (:import [org.apache.spark.sql SQLContext Row]))


(quote

 (defn spark-conf-local []
         (-> (conf/spark-conf)
             (conf/spark-ui-enabled "false")
             (conf/master "local")
             (conf/app-name "sparkling")))


 (def ctx  (spark/spark-context (spark-conf-local)))

 (def sqlCtx (sql-context ctx))
 )

                                        ;scala operator method names lookup http://www.codecommit.com/blog/java/interop-between-java-and-scala
                                        ;calling vararg Scala methods http://stackoverflow.com/questions/10060377/how-to-use-scala-varargs-from-java-code
(defn to-scala-varargs [project-fn xs]
  (let [varargs (new scala.collection.mutable.ArrayBuffer)]
         (doseq [x xs]
          (.$plus$eq varargs (project-fn x)))
         varargs))

(defn to-df-columns [df column-names]
  (to-scala-varargs #(.apply df %) column-names))


(defn select [df & col-names]
  (->> (to-df-columns df col-names)
       (.select df)))


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

(defn print-schema [df]
  (-> df
      (.schema)
      (.treeString)
      println))

(defn show [df]
  (-> df
      (.showString 20 true)
      println))

(defn columns [df]
  (->> df
       (.columns)
       (into [])))

(defn group-by [df & column-names]
  (->> (to-df-columns df column-names)
       (.groupBy df)))

(defn df-column [df column-name]
  (.apply df column-name))

(defn apply-operator [df-col op arg]
  ;todo go through Spark's Column.scala and find all operators
  (cond
    (= op =) (.$eq$eq$eq df-col arg)
    (= op not=) (.$bang$eq$eq df-col arg)
    (= op +) (.$plus df-col arg)
    (= op -) (.$minus df-col arg)
    (= op *) (.$times df-col arg)
    (= op /) (.$div df-col arg)
    (= op >) (.$greater df-col arg)
    (= op >=) (.$greater$eq df-col arg)
    (= op <=) (.$less$eq df-col arg)
    (= op <) (.$less df-col arg)))

(defn count [df]
  (.count df))

(defn filter [df col operator operand]
   (.filter df (-> (df-column df col)
                   (apply-operator operator operand))))

(quote
                                        ;unfortunately this fails at cannot resolve symbol $greater...
                                        ;import those $greater.. symbols from scala ?
 ;or macro?
 (defn to-scala-operator [op]
   (cond
     (= op >) $greater
     (= op >=) $greater$eq
     (= op <=) $less$eq
     (= op <) $less))


 (defn filter [df col operator operand]
   (.filter df (-> (df-column df col)
                   (. (to-scala-operator operator) operand)))))
