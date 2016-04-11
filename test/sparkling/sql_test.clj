(ns sparkling.sql-test
  (:import [org.apache.spark.sql SQLContext DataFrame])
  (:use clojure.test)
  (:require [sparkling.conf :as conf]
            [sparkling.api :as api]
            [sparkling.core :as spark]
            [sparkling.sql :as sql]))

(defn spark-conf-local []
  (-> (conf/spark-conf)
      (conf/spark-ui-enabled "false")
      (conf/master "local")
      (conf/app-name "sparkling")))

(def sample-json "resources/people.json")

(deftest sql-context
  (let [conf (spark-conf-local)]
    (api/with-context sc conf
      (testing "gives SqlContext"
        (is (= (class (sql/sql-context sc)) SQLContext))))))

(deftest read-json
  (let [conf (spark-conf-local)]
    (api/with-context sc conf
      (let [sql-context (sql/sql-context sc)]
        (testing "read-json loads DataFrame"
          (= (class (sql/read-json sql-context sample-json)) DataFrame))))))

(deftest sql
  (let [conf (spark-conf-local)]
    (api/with-context sc conf
      (let [sql-context (sql/sql-context sc)
            df (sql/read-json sql-context sample-json)]
        (sql/register-temp-table df "test")
        (testing "gives SqlContext"
          (is (= (class (sql/sql sql-context "select * from test")) DataFrame)))))))
