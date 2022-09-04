(ns konserve-jdbc.io
  "IO function for interacting with database"
  (:require [next.jdbc :as jdbc]
            [next.jdbc.result-set :as rs])
  (:import  [java.io ByteArrayInputStream]
            [java.sql Blob]))

(set! *warn-on-reflection* 1)

(defn extract-bytes [obj dbtype]
  (when obj
    (cond
      (= "h2" dbtype)
        (.getBytes ^Blob obj 0 (.length ^Blob obj))
        :else obj)))

(defn split-header [bytes-or-blob dbtype]
  (when (some? bytes-or-blob) 
    (let [bytes (extract-bytes bytes-or-blob dbtype)
          data  (->> bytes vec (split-at 4))
          streamer (fn [header data] (list (byte-array header) (-> data byte-array (ByteArrayInputStream.))))]
      (apply streamer data))))
    

(defn it-exists? 
  [store id]
  (let [res (first (jdbc/execute! (:conn store) [(str "select 1 from " (:table store) " where id = '" id "'")]))]
    (not (nil? res))))

(defn get-it 
  [store id]
  (let [res' (first (jdbc/execute! (:conn store) [(str "select * from " (:table store) " where id = '" id "'")] {:builder-fn rs/as-unqualified-lower-maps}))
        data (:data res')
        meta (:meta res')
        res (if (and meta data)
              [(split-header meta (-> store :db :dbtype)) (split-header data  (-> store :db :dbtype))]
              [nil nil])]
    res))

(defn get-it-only
  [store id]
  (let [res' (first (jdbc/execute! (:conn store) [(str "select id,data from " (:table store) " where id = '" id "'")] {:builder-fn rs/as-unqualified-lower-maps}))
        data (:data res')
        res (when data (split-header data  (-> store :db :dbtype)))]
    res))

(defn get-meta 
  [store id]
  (let [res' (first (jdbc/execute! (:conn store) [(str "select id,meta from " (:table store) " where id = '" id "'")] {:builder-fn rs/as-unqualified-lower-maps}))
        meta (:meta res')
        res (when meta (split-header meta  (-> store :db :dbtype)))]
    res))

(defn update-it 
  [store id data]
  (let [ps (jdbc/prepare (:conn store) [(str "update " (:table store) " set meta = ?, data = ? where id = ?") 
                                    (first data)
                                    (second data)
                                    id])]
    (jdbc/execute-one! ps)))

(defn insert-it 
  [store id data]
  (let [ps (jdbc/prepare (:conn store) [(str "insert into " (:table store) " (id,meta,data) values(?, ?, ?)")
                                    id
                                    (first data)
                                    (second data)])]
    (jdbc/execute-one! ps)))

(defn delete-it 
  [store id]
  (jdbc/execute! (:ds store) [(str "delete from " (:table store) " where id = '" id "'")])) 

(defn get-keys 
  [store]
  (let [res' (jdbc/execute! (:conn store) [(str "select id,meta from " (:table store))] {:builder-fn rs/as-unqualified-lower-maps})
        res (doall (map #(split-header (:meta %) (-> store :db :dbtype)) res'))]
    res))

(defn raw-get-it-only
  [store id]
  (let [res' (first (jdbc/execute! (:conn store) [(str "select id,data from " (:table store) " where id = '" id "'")] {:builder-fn rs/as-unqualified-lower-maps}))
        data (:data res')]
    (extract-bytes data  (-> store :db :dbtype))))

(defn raw-get-meta 
  [store id]
  (let [res' (first (jdbc/execute! (:conn store) [(str "select id,meta from " (:table store) " where id = '" id "'")] {:builder-fn rs/as-unqualified-lower-maps}))
        meta (:meta res')]
    (extract-bytes meta (-> store :db :dbtype))))

(defn raw-update-it-only 
  [store id blob]
  (let [ps (jdbc/prepare (:conn store) [(str "update " (:table store) " set data = ? where id = ?") 
                                    blob
                                    id])]
    (jdbc/execute-one! ps)))

(defn raw-insert-it-only
  [store id blob]
  (let [ps (jdbc/prepare (:conn store) [(str "insert into " (:table store) " (id,data) values(?, ?)")
                                    id
                                    blob])]
    (jdbc/execute-one! ps)))

(defn raw-update-meta
  [store id blob]
  (let [ps (jdbc/prepare (:conn store) [(str "update " (:table store) " set meta = ? where id = ?") 
                                    blob
                                    id])]
    (jdbc/execute-one! ps)))

(defn raw-insert-meta
  [store id blob]
  (let [ps (jdbc/prepare (:conn store) [(str "insert into " (:table store) " (id,meta) values(?, ?)")
                                    id
                                    blob])]
    (jdbc/execute-one! ps)))