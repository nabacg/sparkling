(ns sparkling.sql-test
  (:import [org.apache.spark.sql SQLContext DataFrame]
           [org.apache.spark.sql.catalyst.plans.logical.Project])
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
;todo rewrite tests with something like with/test-df
(def sample-json "resources/people.json")

(deftest sql-context
  (let [conf (spark-conf-local)]
    (api/with-context sc conf
      (testing "gives SqlContext"
        (is (= (class (sql/sql-context sc)) SQLContext))))))

(deftest read-json
  (let [conf (spark-conf-local)]
    (api/with-context sc conf
      (let [sql-context (sql/sql-context sc)
            df (sql/read-json sql-context sample-json)]
        (testing "read-json loads DataFrame"
          (= (class df) DataFrame)
          (= (sql/count df) 3))))))

(deftest sql
  (let [conf (spark-conf-local)]
    (api/with-context sc conf
      (let [sql-context (sql/sql-context sc)
            df (sql/read-json sql-context sample-json)
            _ (sql/register-temp-table df "people")
            sql-df (sql/sql sql-context "select * from people where age > 20")]
        (testing "Can execute SQL queries against DataFrame"
          (is (= (class sql-df) DataFrame))
          (= (.count sql-df) 10))))))

(deftest columns
  (api/with-context sc (spark-conf-local)
    (let [df (sql/read-json (sql/sql-context sc) sample-json)]
      (testing "should return clojure vector with all column names"
        (= (sql/columns df) ["name" "age"])))))

(deftest select
  (api/with-context sc (spark-conf-local)
    (let [df (sql/read-json (sql/sql-context sc) sample-json)]
      (testing "select returns dataframe with only columns provided in select call"
        (= (sql/columns (sql/select df "name")) ["name"])))))

(deftest filter
  (api/with-context sc (spark-conf-local)
    (let [df (sql/read-json (sql/sql-context sc) sample-json)]
      (testing "applies filter expression to DF"
        (= (-> (sql/filter df "age" > 19) sql/count) 1)
        (= (-> (sql/filter df "age" >= 19) sql/count) 2)
        (= (-> (sql/filter df "age" < 30) sql/count) 1)
        (= (-> (sql/filter df "age" <= 19) sql/count) 2)))))

(deftest group-by
  (api/with-context sc (spark-conf-local)
    (let [df (sql/read-json (sql/sql-context sc) sample-json)
          grouped (-> (sql/group-by df "age") sql/count)]
      (testing "select returns dataframe with only columns provided in select call"
        (= (-> grouped sql/columns) ["age" "count"])
        (= (-> grouped (.count)) 3)))))
