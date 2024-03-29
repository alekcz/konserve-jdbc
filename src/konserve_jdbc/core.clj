(ns konserve-jdbc.core
  "Address globally aggregated immutable key-value stores(s)."
  (:require [clojure.core.async :as async]
            [konserve.serializers :as ser]
            [konserve.compressor :as comp]
            [konserve.encryptor :as encr]
            [hasch.core :as hasch]
            [konserve-jdbc.io :as io]
            [next.jdbc :as jdbc]
            [next.jdbc.connection :as connection]
            [clojure.string :as str]
            [konserve.protocols :refer [PEDNAsyncKeyValueStore
                                        -exists? -get -get-meta
                                        -update-in -assoc-in -dissoc
                                        PBinaryAsyncKeyValueStore
                                        -bassoc -bget
                                        -serialize -deserialize
                                        PKeyIterable
                                        -keys]]
            [konserve.storage-layout :refer [SplitLayout]])
  (:import  [java.io ByteArrayOutputStream]
            [com.mchange.v2.c3p0 ComboPooledDataSource PooledDataSource]))

(set! *warn-on-reflection* 1)
(def dbtypes ["h2" "h2:mem" "hsqldb" "jtds:sqlserver" "mysql" "oracle:oci" "oracle:thin" "postgresql" "redshift" "sqlite" "sqlserver" "mssql"])
(def store-layout 1)
(def max-buffer-size 16384)
(defonce pool (atom nil))

(defn str-uuid 
  [key] 
  (str (hasch/uuid key))) 

(defn prep-ex 
  [store ^String message ^Exception e]
  (when (-> store :db :debug) 
    (.printStackTrace e))
  (ex-info message {:error (.getMessage e) :cause (.getCause e) :trace (.getStackTrace e)}))

(defn prep-stream 
  [stream]
  { :input-stream stream
    :size nil})

(defn pool-key [db]
  (keyword
    (str-uuid (select-keys db [:dbtype :jdbcUrl :host :port :user :password :dbname]))))

(defrecord JDBCStore [store default-serializer serializers compressor encryptor read-handlers write-handlers locks]
  PEDNAsyncKeyValueStore
  (-exists? 
    [_this key] 
      (let [res-ch (async/chan 1)]
        (async/go
          (try
            (async/put! res-ch (io/it-exists? store (str-uuid key)))
            (catch Exception e (async/put! res-ch (prep-ex store "Failed to determine if item exists" e)))
            (finally (async/close! res-ch))))
        res-ch))

  (-get 
    [_this key] 
    (let [res-ch (async/chan 1)]
      (async/go
        (try
          (let [[header res] (io/get-it-only store (str-uuid key))]
            (when (some? res) 
              (let [rserializer (ser/byte->serializer (get header 1))
                    rcompressor (comp/byte->compressor (get header 2))
                    rencryptor  (encr/byte->encryptor  (get header 3))
                    reader (-> rserializer rencryptor rcompressor)
                    data (-deserialize reader read-handlers res)]
                (async/put! res-ch data))))
          (catch Exception e (async/put! res-ch (prep-ex store "Failed to retrieve value from store" e)))
          (finally (async/close! res-ch))))
      res-ch))

  (-get-meta 
    [_this key] 
    (let [res-ch (async/chan 1)]
      (async/go
        (try
          (let [[header res] (io/get-meta store (str-uuid key))]
            (when (some? res) 
              (let [rserializer (ser/byte->serializer (get header 1))
                    rcompressor (comp/byte->compressor (get header 2))
                    rencryptor  (encr/byte->encryptor  (get header 3))
                    reader (-> rserializer rencryptor rcompressor)
                    data (-deserialize reader read-handlers res)] 
                (async/put! res-ch data))))
          (catch Exception e (async/put! res-ch (prep-ex store "Failed to retrieve metadata from store" e)))
          (finally (async/close! res-ch))))
      res-ch))

  (-update-in 
    [_this key-vec meta-up-fn up-fn args]
    (let [res-ch (async/chan 1)]
      (async/go
        (try
          (let [[fkey & rkey] key-vec
                [[mheader ometa'] [vheader oval']] (io/get-it store (str-uuid fkey))
                old-val [(when ometa'
                            (let [mserializer (ser/byte->serializer  (get mheader 1))
                                  mcompressor (comp/byte->compressor (get mheader 2))
                                  mencryptor  (encr/byte->encryptor  (get mheader 3))
                                  reader (-> mserializer mencryptor mcompressor)]
                              (-deserialize reader read-handlers ometa')))
                         (when oval'
                            (let [vserializer (ser/byte->serializer  (get vheader 1))
                                  vcompressor (comp/byte->compressor (get vheader 2))
                                  vencryptor  (encr/byte->encryptor  (get vheader 3))
                                  reader (-> vserializer vencryptor vcompressor)]
                              (-deserialize reader read-handlers oval')))]            
                [nmeta nval] [(meta-up-fn (first old-val)) 
                              (if rkey (apply update-in (second old-val) rkey up-fn args) (apply up-fn (second old-val) args))]
                serializer (get serializers default-serializer)
                writer (-> serializer compressor encryptor)
                ^ByteArrayOutputStream mbaos (ByteArrayOutputStream.)
                ^ByteArrayOutputStream vbaos (ByteArrayOutputStream.)]
            (when nmeta 
              (.write mbaos ^byte store-layout)
              (.write mbaos ^byte (ser/serializer-class->byte (type serializer)))
              (.write mbaos ^byte (comp/compressor->byte compressor))
              (.write mbaos ^byte (encr/encryptor->byte encryptor))
              (-serialize writer mbaos write-handlers nmeta))
            (when nval 
              (.write vbaos ^byte store-layout)
              (.write vbaos ^byte (ser/serializer-class->byte (type serializer)))
              (.write vbaos ^byte (comp/compressor->byte compressor))
              (.write vbaos ^byte (encr/encryptor->byte encryptor))
              (-serialize writer vbaos write-handlers nval))    
            (if (first old-val)
              (io/update-it store (str-uuid fkey) [(.toByteArray mbaos) (.toByteArray vbaos)])
              (io/insert-it store (str-uuid fkey) [(.toByteArray mbaos) (.toByteArray vbaos)]))
            (async/put! res-ch [(second old-val) nval]))
          (catch Exception e (async/put! res-ch (prep-ex store "Failed to update/write value in store" e)))
          (finally (async/close! res-ch))))
        res-ch))

  (-assoc-in [this key-vec meta val] (-update-in this key-vec meta (fn [_] val) []))

  (-dissoc 
    [_this key] 
    (let [res-ch (async/chan 1)]
      (async/go
        (try
          (io/delete-it store (str-uuid key))
          (async/close! res-ch)
          (catch Exception e (async/put! res-ch (prep-ex store "Failed to delete key-value pair from store" e)))
          (finally (async/close! res-ch))))
        res-ch))

  PBinaryAsyncKeyValueStore
  (-bget 
    [_this key locked-cb]
    (let [res-ch (async/chan 1)]
      (async/go
        (try
          (let [[header res] (io/get-it-only store (str-uuid key))]
            (when (some? res) 
              (let [rserializer (ser/byte->serializer (get header 1))
                    rcompressor (comp/byte->compressor (get header 2))
                    rencryptor  (encr/byte->encryptor  (get header 3))
                    reader (-> rserializer rencryptor rcompressor)
                    data (-deserialize reader read-handlers res)]
                (async/put! res-ch (locked-cb (prep-stream data))))))
          (catch Exception e (async/put! res-ch (prep-ex store "Failed to retrieve binary value from store" e)))
          (finally (async/close! res-ch))))
      res-ch))

  (-bassoc 
    [_this key meta-up-fn input]
    (let [res-ch (async/chan 1)]
      (async/go
        (try
          (let [[[mheader old-meta'] [_ old-val]] (io/get-it store (str-uuid key))
                old-meta (when old-meta' 
                            (let [mserializer (ser/byte->serializer  (get mheader 1))
                                  mcompressor (comp/byte->compressor (get mheader 2))
                                  mencryptor  (encr/byte->encryptor  (get mheader 3))
                                  reader (-> mserializer mencryptor mcompressor)]
                              (-deserialize reader read-handlers old-meta')))           
                new-meta (meta-up-fn old-meta) 
                serializer (get serializers default-serializer)
                writer (-> serializer compressor encryptor)
                ^ByteArrayOutputStream mbaos (ByteArrayOutputStream.)
                ^ByteArrayOutputStream vbaos (ByteArrayOutputStream.)]
            (when new-meta 
              (.write mbaos ^byte store-layout)
              (.write mbaos ^byte (ser/serializer-class->byte (type serializer)))
              (.write mbaos ^byte (comp/compressor->byte compressor))
              (.write mbaos ^byte (encr/encryptor->byte encryptor))
              (-serialize writer mbaos write-handlers new-meta))
            (when input
              (.write vbaos ^byte store-layout)
              (.write vbaos ^byte (ser/serializer-class->byte (type serializer)))
              (.write vbaos ^byte (comp/compressor->byte compressor))
              (.write vbaos ^byte (encr/encryptor->byte encryptor))
              (-serialize writer vbaos write-handlers input))  
            (if old-meta
              (io/update-it store (str-uuid key) [(.toByteArray mbaos) (.toByteArray vbaos)])
              (io/insert-it store (str-uuid key) [(.toByteArray mbaos) (.toByteArray vbaos)]))
            (async/put! res-ch [old-val input]))
          (catch Exception e (async/put! res-ch (prep-ex store "Failed to write binary value in store" e)))
          (finally (async/close! res-ch))))
        res-ch))

  PKeyIterable
  (-keys 
    [_]
    (let [res-ch (async/chan (async/buffer max-buffer-size))]
      (async/go
        (try
          (let [key-stream (io/get-keys store)
                keys' (when key-stream
                        (for [[header k] key-stream]
                          (let [rserializer (ser/byte->serializer (get header 1))
                                rcompressor (comp/byte->compressor (get header 2))
                                rencryptor  (encr/byte->encryptor  (get header 3))
                                reader (-> rserializer rencryptor rcompressor)]
                            (-deserialize reader read-handlers k))))
                keys (map :key keys')]
            (dorun
              (map #(async/put! res-ch %) keys))) 
          (catch Exception e (async/put! res-ch (prep-ex store "Failed to retrieve keys from store" e)))
          (finally (async/close! res-ch))))
        res-ch))
        
  SplitLayout      
  (-get-raw-meta [_this key]
    (let [res-ch (async/chan 1)]
      (async/go
        (try
          (let [res (io/raw-get-meta store (str-uuid key))]
            (when res
              (async/put! res-ch res)))
          (catch Exception e (async/put! res-ch (prep-ex store "Failed to retrieve raw metadata from store" e)))
          (finally (async/close! res-ch))))
      res-ch))

  (-put-raw-meta [_this key blob]
    (let [res-ch (async/chan 1)]
      (async/go
        (try
          (if (io/it-exists? store (str-uuid key))
            (io/raw-update-meta store (str-uuid key) blob)
            (io/raw-insert-meta store (str-uuid key) blob))
          (catch Exception e (async/put! res-ch (prep-ex store "Failed to write raw metadata to store" e)))
          (finally (async/close! res-ch))))
      res-ch))

  (-get-raw-value [_this key]
    (let [res-ch (async/chan 1)]
      (async/go
        (try
          (let [res (io/raw-get-it-only store (str-uuid key))]
            (when res
              (async/put! res-ch res)))
          (catch Exception e (async/put! res-ch (prep-ex store "Failed to retrieve raw value from store" e)))
          (finally (async/close! res-ch))))
      res-ch))

  (-put-raw-value [_this key blob]
    (let [res-ch (async/chan 1)]
      (async/go
        (try
          (if (io/it-exists? store (str-uuid key))
            (io/raw-update-it-only store (str-uuid key) blob)
            (io/raw-insert-it-only store (str-uuid key) blob))
          (catch Exception e (async/put! res-ch (prep-ex store "Failed to write raw value to store" e)))
          (finally (async/close! res-ch))))
      res-ch)))

(defn- clean-jdbcUrl [db]
  (if-not (contains? db :jdbcUrl)
    db
    (let [[auth user password] (re-find #"//(.*):(.*)@" (:jdbcUrl db))
          jdbcUrl (str "jdbc:"
                    (-> (str/replace (:jdbcUrl db) #"jdbc:" "") 
                        (str/replace "postgres://" "postgresql://")))
          jdbcUrl (if auth (str/replace jdbcUrl auth "//") jdbcUrl)
          [_ dbtype] (re-find #"jdbc:(.*)://" jdbcUrl)
      ndb (assoc db :jdbcUrl jdbcUrl :dbtype dbtype)]
      (if (not-any? str/blank? [user password])
        (assoc ndb :user user :password password)
        ndb))))

(defn- build-pool [db id]
  (when-not (or (:dbtype db) (:subprotocol db) (:jdbcUrl db))
    (throw (ex-info ":dbtype must be explicitly declared or a JDBC URL must be provided" {:options dbtypes})))

  (when (nil? (get @pool id)) 
    (let [conns ^PooledDataSource (connection/->pool ComboPooledDataSource db)
          shutdown (fn [] (.close ^PooledDataSource conns))] 
      (swap! pool assoc id conns)
      (.close (jdbc/get-connection conns))
      (.addShutdownHook (Runtime/getRuntime) 
        (Thread. ^Runnable shutdown)))))

(defn- create-table! [db id table]
  (let [res  (try (jdbc/execute! (get @pool id) [(str "select 1 from " table " limit 1")]) (catch Exception _e []))]
    (when (empty? res)
      (case (:dbtype db)
        "postgresql" 
          (jdbc/execute! (get @pool id) [(str "create table if not exists " table " (id varchar(100) primary key, meta bytea, data bytea)")])

        ("mssql" "sqlserver")
          (jdbc/execute! (get @pool id) [(str "IF OBJECT_ID(N'dbo." table  "', N'U') IS NULL BEGIN  CREATE TABLE dbo." table " (id varchar(100) primary key, meta varbinary(max), data varbinary(max)); END;")])
                
        (jdbc/execute! (get @pool id) [(str "create table if not exists " table " (id varchar(100) primary key, meta longblob, data longblob)")])))))
 
(defn new-jdbc-store
  ([db & {:keys [table debug default-serializer serializers compressor encryptor read-handlers write-handlers]
                  :or {default-serializer :FressianSerializer
                        table "konserve"
                        debug false
                        compressor comp/null-compressor
                        encryptor encr/null-encryptor
                        read-handlers (atom {})
                        write-handlers (atom {})}}]     
                           
    (when-not (some true? [(:debug db) debug])
      (System/setProperties 
        (doto (java.util.Properties. (System/getProperties))
          (.put "com.mchange.v2.log.MLog" "com.mchange.v2.log.FallbackMLog")
          (.put "com.mchange.v2.log.FallbackMLog.DEFAULT_CUTOFF_LEVEL" "OFF")))) 

    (let [res-ch (async/chan 1)]    
      (async/go 
        (try
          (let [db  (-> db 
                      (clean-jdbcUrl)
                      (assoc  :testConnectionOnCheckout true)
                      (update :debug #(or (:debug %) debug)))
                actual-table (str "konserve_" (or (:table db) table))
                final-table (str/replace actual-table #"[^0-9a-zA-Z:_]+" "_")
                final-serializer (or (:serializer db) default-serializer)
                final-compressor (or (:compressor db) compressor)
                final-encryptor  (or (:encryptor db) encryptor)   
                id (pool-key db)
                _ (build-pool db id)
                _ (create-table! db id final-table)
                store (map->JDBCStore 
                        { :store {:id id :db db :table final-table :conn pool}
                          :default-serializer final-serializer
                          :serializers (merge ser/key->serializer serializers)
                          :compressor final-compressor
                          :encryptor final-encryptor
                          :read-handlers read-handlers
                          :write-handlers write-handlers
                          :locks (atom {})})]
            (async/put! res-ch store))
          (catch Exception e (async/put! res-ch (prep-ex {:store db} "Failed to connect to store" e)))
          (finally (async/close! res-ch))))
      res-ch)))

(defn delete-store [jdbc-store]
  (let [res-ch (async/chan 1)]
    (async/go
      (try
        (jdbc/execute! (-> jdbc-store :store :db) [(str "drop table " (-> jdbc-store :store :table))])
        (catch Exception e (async/put! res-ch (prep-ex (-> jdbc-store :store) "Failed to delete store" e)))
        (finally (async/close! res-ch))))          
    res-ch))