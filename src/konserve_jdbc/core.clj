(ns konserve-jdbc.core
  "Address globally aggregated immutable key-value stores(s)."
  (:require [clojure.core.async :as async]
            [konserve.serializers :as ser]
            [konserve.compressor :as comp]
            [konserve.encryptor :as encr]
            [hasch.core :as hasch]
            [konserve-jdbc.io :as io]
            [next.jdbc :as jdbc]
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
  (:import  [java.io ByteArrayOutputStream]))

(set! *warn-on-reflection* 1)
(def dbtypes ["h2" "h2:mem" "hsqldb" "jtds:sqlserver" "mysql" "oracle:oci" "oracle:thin" "postgresql" "redshift" "sqlite" "sqlserver" "mssql"])
(def store-layout 1)

(defn str-uuid 
  [key] 
  (str (hasch/uuid key))) 

(defn prep-ex 
  [^String message ^Exception e]
  ;; (.printStackTrace e)
  (ex-info message {:error (.getMessage e) :cause (.getCause e) :trace (.getStackTrace e)}))

(defn prep-stream 
  [stream]
  { :input-stream stream
    :size nil})

(defrecord JDBCStore [store default-serializer serializers compressor encryptor read-handlers write-handlers locks]
  PEDNAsyncKeyValueStore
  (-exists? 
    [_this key] 
      (let [res-ch (async/chan 1)]
        (async/thread
          (try
            (async/put! res-ch (io/it-exists? store (str-uuid key)))
            (catch Exception e (async/put! res-ch (prep-ex "Failed to determine if item exists" e)))
            (finally (async/close! res-ch))))
        res-ch))

  (-get 
    [_this key] 
    (let [res-ch (async/chan 1)]
      (async/thread
        (try
          (let [[header res] (io/get-it-only store (str-uuid key))]
            (when (some? res) 
              (let [rserializer (ser/byte->serializer (get header 1))
                    rcompressor (comp/byte->compressor (get header 2))
                    rencryptor  (encr/byte->encryptor  (get header 3))
                    reader (-> rserializer rencryptor rcompressor)
                    data (-deserialize reader read-handlers res)]
                (async/put! res-ch data))))
          (catch Exception e (async/put! res-ch (prep-ex "Failed to retrieve value from store" e)))
          (finally (async/close! res-ch))))
      res-ch))

  (-get-meta 
    [_this key] 
    (let [res-ch (async/chan 1)]
      (async/thread
        (try
          (let [[header res] (io/get-meta store (str-uuid key))]
            (if (some? res) 
              (let [rserializer (ser/byte->serializer (get header 1))
                    rcompressor (comp/byte->compressor (get header 2))
                    rencryptor  (encr/byte->encryptor  (get header 3))
                    reader (-> rserializer rencryptor rcompressor)
                    data (-deserialize reader read-handlers res)] 
                (async/put! res-ch data))))
          (catch Exception e (async/put! res-ch (prep-ex "Failed to retrieve metadata from store" e)))
          (finally (async/close! res-ch))))
      res-ch))

  (-update-in 
    [_this key-vec meta-up-fn up-fn args]
    (let [res-ch (async/chan 1)]
      (async/thread
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
          (catch Exception e (async/put! res-ch (prep-ex "Failed to update/write value in store" e)))
          (finally (async/close! res-ch))))
        res-ch))

  (-assoc-in [this key-vec meta val] (-update-in this key-vec meta (fn [_] val) []))

  (-dissoc 
    [_this key] 
    (let [res-ch (async/chan 1)]
      (async/thread
        (try
          (io/delete-it store (str-uuid key))
          (async/close! res-ch)
          (catch Exception e (async/put! res-ch (prep-ex "Failed to delete key-value pair from store" e)))
          (finally (async/close! res-ch))))
        res-ch))

  PBinaryAsyncKeyValueStore
  (-bget 
    [_this key locked-cb]
    (let [res-ch (async/chan 1)]
      (async/thread
        (try
          (let [[header res] (io/get-it-only store (str-uuid key))]
            (when (some? res) 
              (let [rserializer (ser/byte->serializer (get header 1))
                    rcompressor (comp/byte->compressor (get header 2))
                    rencryptor  (encr/byte->encryptor  (get header 3))
                    reader (-> rserializer rencryptor rcompressor)
                    data (-deserialize reader read-handlers res)]
                (async/put! res-ch (locked-cb (prep-stream data))))))
          (catch Exception e (async/put! res-ch (prep-ex "Failed to retrieve binary value from store" e)))
          (finally (async/close! res-ch))))
      res-ch))

  (-bassoc 
    [_this key meta-up-fn input]
    (let [res-ch (async/chan 1)]
      (async/thread
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
          (catch Exception e (async/put! res-ch (prep-ex "Failed to write binary value in store" e)))
          (finally (async/close! res-ch))))
        res-ch))

  PKeyIterable
  (-keys 
    [_]
    (let [res-ch (async/chan)]
      (async/thread
        (try
          (let [key-stream (io/get-keys store)
                keys' (when key-stream
                        (for [[header k] key-stream]
                          (let [rserializer (ser/byte->serializer (get header 1))
                                rcompressor (comp/byte->compressor (get header 2))
                                rencryptor  (encr/byte->encryptor  (get header 3))
                                reader (-> rserializer rencryptor rcompressor)]
                            (-deserialize reader read-handlers k))))
                keys (doall (map :key keys'))]
            (doall
              (map #(async/put! res-ch %) keys))) 
          (catch Exception e (async/put! res-ch (prep-ex "Failed to retrieve keys from store" e)))
          (finally (async/close! res-ch))))
        res-ch))
        
  SplitLayout      
  (-get-raw-meta [_this key]
    (let [res-ch (async/chan 1)]
      (async/thread
        (try
          (let [res (io/raw-get-meta store (str-uuid key))]
            (when res
              (async/put! res-ch res)))
          (catch Exception e (async/put! res-ch (prep-ex "Failed to retrieve raw metadata from store" e)))
          (finally (async/close! res-ch))))
      res-ch))

  (-put-raw-meta [_this key blob]
    (let [res-ch (async/chan 1)]
      (async/thread
        (try
          (if (io/it-exists? store (str-uuid key))
            (io/raw-update-meta store (str-uuid key) blob)
            (io/raw-insert-meta store (str-uuid key) blob))
          (catch Exception e (async/put! res-ch (prep-ex "Failed to write raw metadata to store" e)))
          (finally (async/close! res-ch))))
      res-ch))

  (-get-raw-value [_this key]
    (let [res-ch (async/chan 1)]
      (async/thread
        (try
          (let [res (io/raw-get-it-only store (str-uuid key))]
            (when res
              (async/put! res-ch res)))
          (catch Exception e (async/put! res-ch (prep-ex "Failed to retrieve raw value from store" e)))
          (finally (async/close! res-ch))))
      res-ch))

  (-put-raw-value [_this key blob]
    (let [res-ch (async/chan 1)]
      (async/thread
        (try
          (if (io/it-exists? store (str-uuid key))
            (io/raw-update-it-only store (str-uuid key) blob)
            (io/raw-insert-it-only store (str-uuid key) blob))
          (catch Exception e (async/put! res-ch (prep-ex "Failed to write raw value to store" e)))
          (finally (async/close! res-ch))))
      res-ch)))

(defn new-jdbc-store
  ([db & {:keys [table default-serializer serializers compressor encryptor read-handlers write-handlers]
                    :or {default-serializer :FressianSerializer
                         table "konserve"
                         compressor comp/lz4-compressor
                         encryptor encr/null-encryptor
                         read-handlers (atom {})
                         write-handlers (atom {})}}]
    (let [res-ch (async/chan 1)
          dbtype (or (:dbtype db) (:subprotocol db))
          final-table (str "konserve_" (or (:table db) table))
          clean-table (str/replace final-table #"[^0-9a-zA-Z:_]+" "_")]                      
      (async/thread 
        (try
          (when-not dbtype 
              (throw (ex-info ":dbtype must be explicitly declared" {:options dbtypes})))
          (let [datasource (jdbc/get-datasource db)]
            (case dbtype

              "postgresql" 
                (jdbc/execute! datasource [(str "create table if not exists " clean-table " (id varchar(100) primary key, meta bytea, data bytea)")])

              ("mssql" "sqlserver")
                (jdbc/execute! datasource [(str "IF OBJECT_ID(N'dbo." clean-table  "', N'U') IS NULL BEGIN  CREATE TABLE dbo." clean-table " (id varchar(100) primary key, meta varbinary(max), data varbinary(max)); END;")])
            
                (jdbc/execute! datasource [(str "create table if not exists " clean-table " (id varchar(100) primary key, meta longblob, data longblob)")]))
            
            (async/put! res-ch
              (map->JDBCStore { :store {:db db :table clean-table :ds datasource :conn (jdbc/get-connection datasource)}
                                :default-serializer default-serializer
                                :serializers (merge ser/key->serializer serializers)
                                :compressor compressor
                                :encryptor encryptor
                                :read-handlers read-handlers
                                :write-handlers write-handlers
                                :locks (atom {})})))
          (catch Exception e (async/put! res-ch (prep-ex "Failed to connect to store" e)))
          (finally (async/close! res-ch))))
      res-ch)))

(defn release-store [jdbc-store]
  (let [res-ch (async/chan 1)]
    (async/thread
      (try
        (when 
          (-> jdbc-store :store :conn) (.close (-> jdbc-store :store :conn))
          (assoc-in jdbc-store [:store :conn] nil))
        (catch Exception e (async/put! res-ch (prep-ex "Failed to release store" e)))
        (finally (async/close! res-ch))))          
    res-ch))


(defn delete-store [jdbc-store]
  (let [res-ch (async/chan 1)]
    (async/thread
      (try
        (jdbc/execute! (-> jdbc-store :store :ds) [(str "drop table " (-> jdbc-store :store :table))])
        (when (-> jdbc-store :store :conn) 
          (.close (-> jdbc-store :store :conn))
          (assoc-in jdbc-store [:store :conn] nil))
        (catch Exception e (async/put! res-ch (prep-ex "Failed to delete store" e)))
        (finally (async/close! res-ch))))          
    res-ch))
