(ns konserve-jdbc.core
  "Address globally aggregated immutable key-value conn(s)."
  (:require [clojure.core.async :as async]
            [konserve.serializers :as ser]
            [hasch.core :as hasch]
            [clojure.java.jdbc :as j]
            [next.jdbc :as jdbc]
            [next.jdbc.result-set :as rs]
            [konserve.protocols :refer [PEDNAsyncKeyValueStore
                                        -exists? -get -get-meta
                                        -update-in -assoc-in -dissoc
                                        PBinaryAsyncKeyValueStore
                                        -bassoc -bget
                                        -serialize -deserialize
                                        PKeyIterable
                                        -keys]])
  (:import  [java.io ByteArrayInputStream ByteArrayOutputStream]
            [org.h2.jdbc JdbcBlob]))

(set! *warn-on-reflection* 1)
(def dbtypes ["h2" "h2:mem" "hsqldb" "jtds:sqlserver" "mysql" "oracle:oci" "oracle:thin" "postgresql" "redshift" "sqlite" "sqlserver"])
(def store-version 1)
(def serializer 1)
(def compressor 0)
(def encryptor 0)

(defn add-version [bytes]
  (when (seq bytes) 
    (byte-array (into [] (concat 
                            [(byte store-version) (byte serializer) (byte compressor) (byte encryptor)] 
                            (vec bytes))))))

(defn extract-bytes [obj]
  (cond
    (= org.h2.jdbc.JdbcBlob (type obj))
      (.getBytes ^JdbcBlob obj 0 (.length ^JdbcBlob obj))
    :else obj))

(defn strip-version [bytes-or-blob]
  (when (some? bytes-or-blob) 
    (let [bytes (extract-bytes bytes-or-blob)]
      (byte-array (->> bytes vec (split-at 4) second)))))

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
                [(strip-version meta) (strip-version data)]
                [nil nil])]
      res)))

(defn get-it-only
  [conn id]
  (with-open [con (jdbc/get-connection (:ds conn))]
    (let [res' (first (jdbc/execute! con [(str "select id,data from " (:table conn) " where id = '" id "'")] {:builder-fn rs/as-unqualified-lower-maps}))
          data (:data res')
          res (when data (strip-version data))]
      res)))

(defn get-meta 
  [conn id]
  (with-open [con (jdbc/get-connection (:ds conn))]
    (let [res' (first (jdbc/execute! con [(str "select id,meta from " (:table conn) " where id = '" id "'")] {:builder-fn rs/as-unqualified-lower-maps}))
          meta (:meta res')
          res (when meta (strip-version meta))]
      res)))

(defn update-it 
  [conn id data]
  (with-open [con (jdbc/get-connection (:ds conn))]
    (with-open [ps (jdbc/prepare con [(str "update " (:table conn) " set meta = ?, data = ? where id = ?") 
                                      (add-version (first data)) 
                                      (add-version (second data)) 
                                      id])]
      (jdbc/execute-one! ps))))

(defn insert-it 
  [conn id data]
  (with-open [con (jdbc/get-connection (:ds conn))]
    (with-open [ps (jdbc/prepare con [(str "insert into " (:table conn) " (id,meta,data) values(?, ?, ?)")
                                      id
                                      (add-version (first data)) 
                                      (add-version (second data))])]
      (jdbc/execute-one! ps))))

(defn delete-it 
  [conn id]
  (jdbc/execute! (:ds conn) [(str "delete from " (:table conn) " where id = '" id "'")])) 

(defn get-keys 
  [conn]
  (with-open [con (jdbc/get-connection (:ds conn))]
    (let [res' (jdbc/execute! con [(str "select id,meta from " (:table conn))] {:builder-fn rs/as-unqualified-lower-maps})
          res (doall (map #(strip-version (:meta %)) res'))]
      res)))


(defn str-uuid 
  [key] 
  (str (hasch/uuid key))) 

(defn prep-ex 
  [^String message ^Exception e]
  ;(.printStackTrace e)
  (ex-info message {:error (.getMessage e) :cause (.getCause e) :trace (.getStackTrace e)}))

(defn prep-stream 
  [bytes]
  { :input-stream  (ByteArrayInputStream. bytes) 
    :size (count bytes)})

(defrecord JDBCStore [conn serializer read-handlers write-handlers locks]
  PEDNAsyncKeyValueStore
  (-exists? 
    [this key] 
      (let [res-ch (async/chan 1)]
        (async/thread
          (try
            (async/put! res-ch (it-exists? conn (str-uuid key)))
            (catch Exception e (async/put! res-ch (prep-ex "Failed to determine if item exists" e)))))
        res-ch))

  (-get 
    [this key] 
    (let [res-ch (async/chan 1)]
      (async/thread
        (try
          (let [res (get-it-only conn (str-uuid key))]
            (if (some? res) 
              (let [data (-deserialize serializer read-handlers res)]
                (async/put! res-ch data))
              (async/close! res-ch)))
          (catch Exception e (async/put! res-ch (prep-ex "Failed to retrieve value from store" e)))))
      res-ch))

  (-get-meta 
    [this key] 
    (let [res-ch (async/chan 1)]
      (async/thread
        (try
          (let [res (get-meta conn (str-uuid key))]
            (if (some? res) 
              (let [data (-deserialize serializer read-handlers res)] 
                (async/put! res-ch data))
              (async/close! res-ch)))
          (catch Exception e (async/put! res-ch (prep-ex "Failed to retrieve value metadata from store" e)))))
      res-ch))

  (-update-in 
    [this key-vec meta-up-fn up-fn args]
    (let [res-ch (async/chan 1)]
      (async/thread
        (try
          (let [[fkey & rkey] key-vec
                [ometa' oval'] (get-it conn (str-uuid fkey))
                old-val [(when ometa'
                          (-deserialize serializer read-handlers ometa'))
                         (when oval'
                          (-deserialize serializer read-handlers oval'))]            
                [nmeta nval] [(meta-up-fn (first old-val)) 
                              (if rkey (apply update-in (second old-val) rkey up-fn args) (apply up-fn (second old-val) args))]
                ^ByteArrayOutputStream mbaos (ByteArrayOutputStream.)
                ^ByteArrayOutputStream vbaos (ByteArrayOutputStream.)]
            (when nmeta (-serialize serializer mbaos write-handlers nmeta))
            (when nval (-serialize serializer vbaos write-handlers nval))    
            (if (first old-val)
              (update-it conn (str-uuid fkey) [(.toByteArray mbaos) (.toByteArray vbaos)])
              (insert-it conn (str-uuid fkey) [(.toByteArray mbaos) (.toByteArray vbaos)]))
            (async/put! res-ch [(second old-val) nval]))
          (catch Exception e (async/put! res-ch (prep-ex "Failed to update/write value in store" e)))))
        res-ch))

  (-assoc-in [this key-vec meta val] (-update-in this key-vec meta (fn [_] val) []))

  (-dissoc 
    [this key] 
    (let [res-ch (async/chan 1)]
      (async/thread
        (try
          (delete-it conn (str-uuid key))
          (async/close! res-ch)
          (catch Exception e (async/put! res-ch (prep-ex "Failed to delete key-value pair from store" e)))))
        res-ch))

  PBinaryAsyncKeyValueStore
  (-bget 
    [this key locked-cb]
    (let [res-ch (async/chan 1)]
      (async/thread
        (try
          (let [res (get-it-only conn (str-uuid key))]
            (if (some? res) 
              (async/put! res-ch (locked-cb (prep-stream res)))
              (async/close! res-ch)))
          (catch Exception e (async/put! res-ch (prep-ex "Failed to retrieve binary value from store" e)))))
      res-ch))

  (-bassoc 
    [this key meta-up-fn input]
    (let [res-ch (async/chan 1)]
      (async/thread
        (try
          (let [[old-meta' old-val] (get-it conn (str-uuid key))
                old-meta (when old-meta' (-deserialize serializer read-handlers old-meta'))           
                new-meta (meta-up-fn old-meta) 
                ^ByteArrayOutputStream mbaos (ByteArrayOutputStream.)]
            (when new-meta (-serialize serializer mbaos write-handlers new-meta))
            (if old-meta
              (update-it conn (str-uuid key) [(.toByteArray mbaos) input])
              (insert-it conn (str-uuid key) [(.toByteArray mbaos) input]))
            (async/put! res-ch [old-val input]))
          (catch Exception e (async/put! res-ch (prep-ex "Failed to write binary value in store" e)))))
        res-ch))

  PKeyIterable
  (-keys 
    [_]
    (let [res-ch (async/chan)]
      (async/thread
        (try
          (let [key-stream (get-keys conn)
                keys' (when key-stream
                        (for [k key-stream]
                          (let [bais (ByteArrayInputStream. k)]
                            (-deserialize serializer read-handlers bais))))
                keys (doall (map :key keys'))]
            (doall
              (map #(async/put! res-ch %) keys))
            (async/close! res-ch)) 
          (catch Exception e (async/put! res-ch (prep-ex "Failed to retrieve keys from store" e)))))
        res-ch)))


(defn new-jdbc-store
  ([db & {:keys [table serializer read-handlers write-handlers]
                    :or {table "konserve"
                         serializer (ser/fressian-serializer)
                         read-handlers (atom {})
                         write-handlers (atom {})}}]
    (let [res-ch (async/chan 1)]                      
      (async/thread 
        (try
          (let [dbtype (or (:dbtype db) (:subprotocol db))
                datasource (jdbc/get-datasource db)]
            (when-not dbtype 
              (throw (ex-info ":dbtype must be explicitly declared" {:options dbtypes})))
            (case dbtype

              "postgresql" 
                (jdbc/execute! datasource [(str "create table if not exists " table " (id varchar(100) primary key, meta bytea, data bytea)")])
            
                (jdbc/execute! datasource [(str "create table if not exists " table " (id varchar(100) primary key, meta longblob, data longblob)")]))
            
            (async/put! res-ch
              (map->JDBCStore { :conn {:db db :table table :ds datasource}
                                :read-handlers read-handlers
                                :write-handlers write-handlers
                                :serializer serializer
                                :locks (atom {})})))
          (catch Exception e (async/put! res-ch (prep-ex "Failed to connect to store" e)))))
      res-ch)))

(defn delete-store [store]
  (let [res-ch (async/chan 1)]
    (async/thread
      (try
        (j/execute! (-> store :conn :db) [(str "drop table " (-> store :conn :table))])
        (async/close! res-ch)
        (catch Exception e (async/put! res-ch (prep-ex "Failed to delete store" e)))))          
    res-ch))
