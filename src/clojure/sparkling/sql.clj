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

(defn select [df & columnNames]
  (->> (let [varargs (new scala.collection.mutable.ArrayBuffer)]
         (doseq [colName columnNames]
          (.$plus$eq varargs (.apply df colName)))
        varargs)
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


(defn columns [df]
  (->> df
       (.columns)
       (into [])))
