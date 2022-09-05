(ns konserve-jdbc.io
  "IO function for interacting with database"
  (:require [next.jdbc :as jdbc]
            [next.jdbc.result-set :as rs]
            [konserve-jdbc.h2 :as h2])
  (:import  [java.io ByteArrayInputStream]
            [java.sql Blob]
            [com.mchange.v2.c3p0 ComboPooledDataSource PooledDataSource]))

(set! *warn-on-reflection* 1)

(defn h2? [store]
  (-> store :db :dbtype (= "h2")))

(defn extract-bytes [obj store]
  (when obj
    (cond
      (h2? store)
        (.getBytes ^Blob obj 0 (.length ^Blob obj))
        :else obj)))

(defn split-header [bytes-or-blob store]
  (when (some? bytes-or-blob) 
    (let [bytes (extract-bytes bytes-or-blob store)
          data  (->> bytes vec (split-at 4))
          streamer (fn [header data] (list (byte-array header) (-> data byte-array (ByteArrayInputStream.))))]
      (apply streamer data))))
    

(defn it-exists? 
  [store id]
  (if (h2? store)
    (h2/it-exists? store id)
    (let [res (first (jdbc/execute! (:conn store) [(str "select 1 from " (:table store) " where id = '" id "'")]))]
      (not (nil? res)))))

(defn get-it 
  [store id]
  (if (h2? store)
    (h2/get-it store id)
    (let [res' (first (jdbc/execute! (:conn store) [(str "select * from " (:table store) " where id = '" id "'")] {:builder-fn rs/as-unqualified-lower-maps}))
        data (:data res')
        meta (:meta res')
        res (if (and meta data)
              [(split-header meta store) (split-header data  store)]
              [nil nil])]
      res)))

(defn get-it-only
  [store id]
  (if (h2? store)
    (h2/get-it-only store id)
    (let [res' (first (jdbc/execute! (:conn store) [(str "select id,data from " (:table store) " where id = '" id "'")] {:builder-fn rs/as-unqualified-lower-maps}))
        data (:data res')
        res (when data (split-header data store))]
      res)))

(defn get-meta 
  [store id]
  (if (h2? store)
    (h2/get-meta store id)
    (let [res' (first (jdbc/execute! (:conn store) [(str "select id,meta from " (:table store) " where id = '" id "'")] {:builder-fn rs/as-unqualified-lower-maps}))
        meta (:meta res')
        res (when meta (split-header meta store))]
      res)))

(defn update-it 
  [store id data]
  (if (h2? store)
    (h2/update-it store id data)
    (let [ps [(str "update " (:table store) " set meta = ?, data = ? where id = ?") (first data) (second data) id]]
      (jdbc/execute-one! (:conn store) ps))))

(defn insert-it 
  [store id data]
  (if (h2? store)
    (h2/insert-it store id data)
    (let [ps [(str "insert into " (:table store) " (id,meta,data) values(?, ?, ?)") id (first data) (second data)]]
      (jdbc/execute-one! (:conn store) ps))))

(defn delete-it 
  [store id]
  (if (h2? store)
    (h2/delete-it store id)
    (jdbc/execute! (:conn store) [(str "delete from " (:table store) " where id = '" id "'")])) )

(defn get-keys 
  [store]
  (if (h2? store)
    (h2/get-keys store)
    (let [res' (jdbc/execute! (:conn store) [(str "select id,meta from " (:table store))] {:builder-fn rs/as-unqualified-lower-maps})
        res (doall (map #(split-header (:meta %) store) res'))]
      res)))

(defn raw-get-it-only
  [store id]
  (if (h2? store)
    (h2/raw-get-it-only store id)
    (let [res' (first (jdbc/execute! (:conn store) [(str "select id,data from " (:table store) " where id = '" id "'")] {:builder-fn rs/as-unqualified-lower-maps}))
        data (:data res')]
      (extract-bytes data store))))

(defn raw-get-meta 
  [store id]
  (if (h2? store)
    (h2/raw-get-meta store id)
    (let [res' (first (jdbc/execute! (:conn store) [(str "select id,meta from " (:table store) " where id = '" id "'")] {:builder-fn rs/as-unqualified-lower-maps}))
        meta (:meta res')]
      (extract-bytes meta store))))

(defn raw-update-it-only 
  [store id blob]
  (if (h2? store)
    (h2/raw-update-it-only store id blob)
    (let [ps [(str "update " (:table store) " set data = ? where id = ?") blob id]]
      (jdbc/execute-one! (:conn store) ps))))

(defn raw-insert-it-only
  [store id blob]
  (if (h2? store)
    (h2/raw-insert-it-only store id blob)
    (let [ps [(str "insert into " (:table store) " (id,data) values(?, ?)") id blob]]
      (jdbc/execute-one! (:conn store) ps))))

(defn raw-update-meta
  [store id blob]
  (if (h2? store)
    (h2/raw-update-meta store id blob)
    (let [ps [(str "update " (:table store) " set meta = ? where id = ?") blob id]]
      (jdbc/execute-one! (:conn store) ps))))

(defn raw-insert-meta
  [store id blob]
  (if (h2? store)
    (h2/raw-insert-meta store id blob)
    (let [ps [(str "insert into " (:table store) " (id,meta) values(?, ?)") id blob]]
      (jdbc/execute-one! (:conn store) ps))))