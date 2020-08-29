(ns konserve-jdbc.core
  "Address globally aggregated immutable key-value conn(s)."
  (:require [clojure.core.async :as async]
            [konserve.serializers :as ser]
            [konserve.compressor :as comp]
            [konserve.encryptor :as encr]
            [hasch.core :as hasch]
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
(def store-layout 1)
(def serializer-byte 1)
(def compressor-byte 0)
(def encryptor-byte 0)

(defn extract-bytes [obj]
  (cond
    (= org.h2.jdbc.JdbcBlob (type obj))
      (.getBytes ^JdbcBlob obj 0 (.length ^JdbcBlob obj))
    :else obj))

(defn split-header [bytes-or-blob]
  (when (some? bytes-or-blob) 
    (let [bytes (extract-bytes bytes-or-blob)]
      (map byte-array (->> bytes vec (split-at 4))))))

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
                [(split-header meta) (split-header data)]
                [nil nil])]
      res)))

(defn get-it-only
  [conn id]
  (with-open [con (jdbc/get-connection (:ds conn))]
    (let [res' (first (jdbc/execute! con [(str "select id,data from " (:table conn) " where id = '" id "'")] {:builder-fn rs/as-unqualified-lower-maps}))
          data (:data res')
          res (when data (split-header data))]
      res)))

(defn get-meta 
  [conn id]
  (with-open [con (jdbc/get-connection (:ds conn))]
    (let [res' (first (jdbc/execute! con [(str "select id,meta from " (:table conn) " where id = '" id "'")] {:builder-fn rs/as-unqualified-lower-maps}))
          meta (:meta res')
          res (when meta (split-header meta))]
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
          res (doall (map #(split-header (:meta %)) res'))]
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

(defrecord JDBCStore [conn default-serializer serializers compressor encryptor read-handlers write-handlers locks]
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
          (let [[header res] (get-it-only conn (str-uuid key))
                serializer (get serializers default-serializer)
                reader (-> serializer identity)]
            (if (some? res) 
              (let [data (-deserialize reader read-handlers res)]
                (async/put! res-ch data))
              (async/close! res-ch)))
          (catch Exception e (async/put! res-ch (prep-ex "Failed to retrieve value from store" e)))))
      res-ch))

  (-get-meta 
    [this key] 
    (let [res-ch (async/chan 1)]
      (async/thread
        (try
          (let [[header res] (get-meta conn (str-uuid key))
                serializer (get serializers default-serializer)
                reader (-> serializer identity)]
            (if (some? res) 
              (let [data (-deserialize reader read-handlers res)] 
                (async/put! res-ch data))
              (async/close! res-ch)))
          (catch Exception e (async/put! res-ch (prep-ex "Failed to retrieve value metadata from store" e)))))
      res-ch))

  (-update-in 
    [this key-vec meta-up-fn up-fn args]
    (let [res-ch (async/chan 1)]
      (async/thread
        (try
          (let [serializer (get serializers default-serializer)
                reader (-> serializer identity)
                writer (-> serializer identity)
                [fkey & rkey] key-vec
                [[metaheader ometa'] [valheader oval']] (get-it conn (str-uuid fkey))
                old-val [(when ometa'
                          (-deserialize reader read-handlers ometa'))
                         (when oval'
                          (-deserialize reader read-handlers oval'))]            
                [nmeta nval] [(meta-up-fn (first old-val)) 
                              (if rkey (apply update-in (second old-val) rkey up-fn args) (apply up-fn (second old-val) args))]
                ^ByteArrayOutputStream mbaos (ByteArrayOutputStream.)
                ^ByteArrayOutputStream vbaos (ByteArrayOutputStream.)]
            (print "header" 
              store-layout 
              (ser/serializer-class->byte (type serializer))
              (comp/compressor->byte compressor)
              (encr/encryptor->byte encryptor))
            (when nmeta 
              (.write mbaos ^byte (byte store-layout))
              (.write mbaos ^byte (byte (ser/serializer-class->byte (type serializer))))
              (.write mbaos ^byte (byte (comp/compressor->byte compressor)))
              (.write mbaos ^byte (byte (encr/encryptor->byte encryptor)))
              (-serialize writer mbaos write-handlers nmeta))
            (when nval 
              (.write vbaos ^byte (byte store-layout))
              (.write vbaos ^byte (byte (ser/serializer-class->byte (type serializer))))
              (.write vbaos ^byte (byte (comp/compressor->byte compressor)))
              (.write vbaos ^byte (byte (encr/encryptor->byte encryptor)))
              (-serialize writer vbaos write-handlers nval))    
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
          (let [serializer (get serializers default-serializer)
                reader (-> serializer identity)
                [header res] (get-it-only conn (str-uuid key))]
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
          (let [serializer (get serializers default-serializer)
                reader (-> serializer identity)
                writer (-> serializer identity)
                [[metaheader old-meta'] [valheader old-val]] (get-it conn (str-uuid key))
                old-meta (when old-meta' (-deserialize reader read-handlers old-meta'))           
                new-meta (meta-up-fn old-meta) 
                ^ByteArrayOutputStream mbaos (ByteArrayOutputStream.)
                ^ByteArrayOutputStream vbaos (ByteArrayOutputStream.)]
            (when new-meta 
              (.write mbaos ^byte (byte store-layout))
              (.write mbaos ^byte (byte (ser/serializer-class->byte (type serializer))))
              (.write mbaos ^byte (byte (comp/compressor->byte compressor)))
              (.write mbaos ^byte (byte (encr/encryptor->byte encryptor)))
              (-serialize writer mbaos write-handlers new-meta))
            (when input
              (.write vbaos ^byte (byte store-layout))
              (.write vbaos ^byte (byte (ser/serializer-class->byte (type serializer))))
              (.write vbaos ^byte (byte (comp/compressor->byte compressor)))
              (.write vbaos ^byte (byte (encr/encryptor->byte encryptor)))
              (.write vbaos input 0 (count input)))  
            (if old-meta
              (update-it conn (str-uuid key) [(.toByteArray mbaos) (.toByteArray vbaos)])
              (insert-it conn (str-uuid key) [(.toByteArray mbaos) (.toByteArray vbaos)]))
            (async/put! res-ch [old-val input]))
          (catch Exception e (async/put! res-ch (prep-ex "Failed to write binary value in store" e)))))
        res-ch))

  PKeyIterable
  (-keys 
    [_]
    (let [res-ch (async/chan)]
      (async/thread
        (try
          (let [serializer (get serializers default-serializer)
                reader (-> serializer identity)
                key-stream (get-keys conn)
                keys' (when key-stream
                        (for [[kheader k] key-stream]
                          (let [bais (ByteArrayInputStream. k)]
                            (-deserialize reader read-handlers bais))))
                keys (doall (map :key keys'))]
            (doall
              (map #(async/put! res-ch %) keys))
            (async/close! res-ch)) 
          (catch Exception e (async/put! res-ch (prep-ex "Failed to retrieve keys from store" e)))))
        res-ch)))


(defn new-jdbc-store
  ([db & {:keys [table default-serializer serializers compressor encryptor read-handlers write-handlers]
                    :or {default-serializer :FressianSerializer
                         table "konserve"
                         compressor comp/null-compressor
                         encryptor encr/null-encryptor
                         read-handlers (atom {})
                         write-handlers (atom {})}}]
    (let [res-ch (async/chan 1)
          dbtype (or (:dbtype db) (:subprotocol db))]                      
      (async/thread 
        (try
          (when-not dbtype 
              (throw (ex-info ":dbtype must be explicitly declared" {:options dbtypes})))
          (let [datasource (jdbc/get-datasource db)]
            (case dbtype

              "postgresql" 
                (jdbc/execute! datasource [(str "create table if not exists " table " (id varchar(100) primary key, meta bytea, data bytea)")])
            
                (jdbc/execute! datasource [(str "create table if not exists " table " (id varchar(100) primary key, meta longblob, data longblob)")]))
            
            (async/put! res-ch
              (map->JDBCStore { :conn {:db db :table table :ds datasource}
                                :default-serializer default-serializer
                                :serializers (merge ser/key->serializer serializers)
                                :compressor compressor
                                :encryptor encryptor
                                :read-handlers read-handlers
                                :write-handlers write-handlers
                                :locks (atom {})})))
          (catch Exception e (async/put! res-ch (prep-ex "Failed to connect to store" e)))))
      res-ch)))

(defn delete-store [store]
  (let [res-ch (async/chan 1)]
    (async/thread
      (try
        (jdbc/execute! (-> store :conn :ds) [(str "drop table " (-> store :conn :table))])
        (async/close! res-ch)
        (catch Exception e (async/put! res-ch (prep-ex "Failed to delete store" e)))))          
    res-ch))
