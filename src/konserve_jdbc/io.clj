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
  [conn id]
  (with-open [con (jdbc/get-connection (:ds conn))]
    (let [res (first (jdbc/execute! con [(str "select 1 from " (:table conn) " where id = '" id "'")]))]
      (not (nil? res)))))

(defn get-it 
  [conn id]
  (with-open [con (jdbc/get-connection (:ds conn))]
    (let [res' (first (jdbc/execute! con [(str "select * from " (:table conn) " where id = '" id "'")] {:builder-fn rs/as-unqualified-lower-maps}))
          data (:data res')
          meta (:meta res')
          res (if (and meta data)
                [(split-header meta (-> conn :db :dbtype)) (split-header data  (-> conn :db :dbtype))]
                [nil nil])]
      res)))

(defn get-it-only
  [conn id]
  (with-open [con (jdbc/get-connection (:ds conn))]
    (let [res' (first (jdbc/execute! con [(str "select id,data from " (:table conn) " where id = '" id "'")] {:builder-fn rs/as-unqualified-lower-maps}))
          data (:data res')
          res (when data (split-header data  (-> conn :db :dbtype)))]
      res)))

(defn get-meta 
  [conn id]
  (with-open [con (jdbc/get-connection (:ds conn))]
    (let [res' (first (jdbc/execute! con [(str "select id,meta from " (:table conn) " where id = '" id "'")] {:builder-fn rs/as-unqualified-lower-maps}))
          meta (:meta res')
          res (when meta (split-header meta  (-> conn :db :dbtype)))]
      res)))

(defn update-it 
  [conn id data]
  (with-open [con (jdbc/get-connection (:ds conn))]
    (with-open [ps (jdbc/prepare con [(str "update " (:table conn) " set meta = ?, data = ? where id = ?") 
                                      (first data)
                                      (second data)
                                      id])]
      (jdbc/execute-one! ps))))

(defn insert-it 
  [conn id data]
  (with-open [con (jdbc/get-connection (:ds conn))]
    (with-open [ps (jdbc/prepare con [(str "insert into " (:table conn) " (id,meta,data) values(?, ?, ?)")
                                      id
                                      (first data)
                                      (second data)])]
      (jdbc/execute-one! ps))))

(defn delete-it 
  [conn id]
  (jdbc/execute! (:ds conn) [(str "delete from " (:table conn) " where id = '" id "'")])) 

(defn get-keys 
  [conn]
  (with-open [con (jdbc/get-connection (:ds conn))]
    (let [res' (jdbc/execute! con [(str "select id,meta from " (:table conn))] {:builder-fn rs/as-unqualified-lower-maps})
          res (doall (map #(split-header (:meta %) (-> conn :db :dbtype)) res'))]
      res)))

(defn raw-get-it-only
  [conn id]
  (with-open [con (jdbc/get-connection (:ds conn))]
    (let [res' (first (jdbc/execute! con [(str "select id,data from " (:table conn) " where id = '" id "'")] {:builder-fn rs/as-unqualified-lower-maps}))
          data (:data res')]
      (extract-bytes data  (-> conn :db :dbtype)))))

(defn raw-get-meta 
  [conn id]
  (with-open [con (jdbc/get-connection (:ds conn))]
    (let [res' (first (jdbc/execute! con [(str "select id,meta from " (:table conn) " where id = '" id "'")] {:builder-fn rs/as-unqualified-lower-maps}))
          meta (:meta res')]
      (extract-bytes meta (-> conn :db :dbtype)))))

(defn raw-update-it-only 
  [conn id blob]
  (with-open [con (jdbc/get-connection (:ds conn))]
    (with-open [ps (jdbc/prepare con [(str "update " (:table conn) " set data = ? where id = ?") 
                                      blob
                                      id])]
      (jdbc/execute-one! ps))))

(defn raw-insert-it-only
  [conn id blob]
  (with-open [con (jdbc/get-connection (:ds conn))]
    (with-open [ps (jdbc/prepare con [(str "insert into " (:table conn) " (id,data) values(?, ?)")
                                      id
                                      blob])]
      (jdbc/execute-one! ps))))      

(defn raw-update-meta
  [conn id blob]
  (with-open [con (jdbc/get-connection (:ds conn))]
    (with-open [ps (jdbc/prepare con [(str "update " (:table conn) " set meta = ? where id = ?") 
                                      blob
                                      id])]
      (jdbc/execute-one! ps))))

(defn raw-insert-meta
  [conn id blob]
  (with-open [con (jdbc/get-connection (:ds conn))]
    (with-open [ps (jdbc/prepare con [(str "insert into " (:table conn) " (id,meta) values(?, ?)")
                                      id
                                      blob])]
      (jdbc/execute-one! ps))))   