(ns konserve-template.string
  "Address globally aggregated immutable key-value store(s)."
  (:require [clojure.core.async :as async]
            [konserve.serializers :as ser]
            [hasch.core :as hasch]
            [konserve.protocols :refer [PEDNAsyncKeyValueStore
                                        -exists? -get -get-meta
                                        -update-in -assoc-in -dissoc
                                        PBinaryAsyncKeyValueStore
                                        -bassoc -bget
                                        -serialize -deserialize
                                        PKeyIterable
                                        -keys]])
  (:import  [java.io ByteArrayInputStream StringWriter]
            [java.util Base64 Base64$Decoder Base64$Encoder]))

(set! *warn-on-reflection* 1)

(def ^Base64$Encoder b64encoder (. Base64 getEncoder))
(def ^Base64$Decoder b64decoder (. Base64 getDecoder))

(defn- reality 
  "This function has nothing to do with konserve. 
   It is used to simulate a latency and faile in interact your store.
   You can remove this function once you've connected konserve to your store"
  [store]
  (Thread/sleep (+ 50 (rand-int 450))) ; random delay to simulate a real i/o. 
  (if (nil? (:auth store)) (throw (Exception. "Boo!")) nil)) 

(defn prep-write 
  "Doc string"
  [data]
  (let [[meta val] data]
    (if (= String (type val))
      {:data data
      :type "string"}
      {:data [meta (.encodeToString b64encoder ^"[B" val)]
      :type "binary"})))

(defn prep-read 
  "Doc string"
  [data']
  (let [data (:data data')
        type (:type data')]
    (case type
      "string" data
      "binary" [(first data) (.decode b64decoder ^String (second data))]
      nil)))

(defn it-exists? 
  "Doc string"
  [store id]
  (reality store) ;simulate store failure
  (contains? (deref (:data store)) id)) ;example
  
(defn get-it 
  "Doc string"
  [store id]
  (reality store) ;simulate store failure
  (prep-read (get (deref (:data store)) id)))

(defn update-it 
  "Doc string"
  [store id data]
  (reality store) ;simulate store failure
  (swap! (:data store) #(assoc % id (prep-write data))))

(defn delete-it 
  "Doc string"
  [store id]
  (reality store) ;simulate store failure
  (swap! (:data store) #(dissoc % id)))

(defn get-keys 
  "Doc string"
  [store]
  (reality store) ;simulate store failure
  (let [keys (seq (vals (deref (:data store))))]
    (map prep-read keys)))

(defn str-uuid 
  "Doc string"
  [key] 
  ;using hasch we create a uuid and convert it to string. 
  ;apply any other restrictions imposed by use store. 
  ;whatever transforms you apply you must insure they are collision free
  (str (hasch/uuid key))) 

(defn prep-ex 
  "Doc string"
  [^String message ^Exception e]
  ; Use print the stack trace when things are going wonky
  ;(.printStackTrace e)
  (ex-info message {:error (.getMessage e) :cause (.getCause e) :trace (.getStackTrace e)}))

(defn prep-stream 
  "Doc string"
  [bytes]
  { :input-stream  (ByteArrayInputStream. bytes) 
    :size (count bytes)})

; Implementation of the konserve protocol starts here.
; All the functions above are helper functions to make the code more readable and 
; maintainable

(defrecord YourStore [store serializer read-handlers write-handlers locks]
  PEDNAsyncKeyValueStore
  (-exists? 
    ;"Doc string"
    [this key] 
      (let [res-ch (async/chan 1)]
        (async/thread
          (try
            (async/put! res-ch (it-exists? store (str-uuid key)))
            (catch Exception e (async/put! res-ch (prep-ex "Failed to determine if item exists" e)))))
        res-ch))

  (-get 
    ;Doc string"
    [this key] 
    (let [res-ch (async/chan 1)]
      (async/thread
        (try
          (let [res (get-it store (str-uuid key))]
            (if (some? res) 
              (async/put! res-ch (-deserialize serializer read-handlers (second res)))
              (async/close! res-ch)))
          (catch Exception e (async/put! res-ch (prep-ex "Failed to retrieve value from store" e)))))
      res-ch))

  (-get-meta 
    ;"Doc string"
    [this key] 
    (let [res-ch (async/chan 1)]
      (async/thread
        (try
          (let [res (get-it store (str-uuid key))]
            (if (some? res) 
              (async/put! res-ch (-deserialize serializer read-handlers (first res)))
              (async/close! res-ch)))
          (catch Exception e (async/put! res-ch (prep-ex "Failed to retrieve value metadata from store" e)))))
      res-ch))

  (-update-in 
    ;"Doc string"
    [this key-vec meta-up-fn up-fn args]
    (let [res-ch (async/chan 1)]
      (async/thread
        (try
          (let [[fkey & rkey] key-vec
                [ometa' oval'] (get-it store (str-uuid fkey))
                old-val [(when ometa'
                          (-deserialize serializer read-handlers ometa'))
                         (when oval'
                          (-deserialize serializer read-handlers oval'))]            
                [nmeta nval] [(meta-up-fn (first old-val)) 
                         (if rkey (apply update-in (second old-val) rkey up-fn args) (apply up-fn (second old-val) args))]
                ^StringWriter mbaos (StringWriter.)
                ^StringWriter vbaos (StringWriter.)]
            (when nmeta (-serialize serializer mbaos write-handlers nmeta))
            (when nval (-serialize serializer vbaos write-handlers nval))
            (update-it store (str-uuid fkey) [(.toString mbaos) (.toString vbaos)])
            (async/put! res-ch [(second old-val) nval]))
          (catch Exception e (async/put! res-ch (prep-ex "Failed to update/write value in store" e)))))
        res-ch))

  (-assoc-in [
    ;"Doc string"
    this key-vec meta val] (-update-in this key-vec meta (fn [_] val) []))

  (-dissoc 
    ;"Doc string"
    [this key] 
    (let [res-ch (async/chan 1)]
      (async/thread
        (try
          (delete-it store (str-uuid key))
          (async/close! res-ch)
          (catch Exception e (async/put! res-ch (prep-ex "Failed to delete key-value pair from store" e)))))
        res-ch))

  PBinaryAsyncKeyValueStore
  (-bget 
    ;"Doc string"
    [this key locked-cb]
    (let [res-ch (async/chan 1)]
      (async/thread
        (try
          (let [res (get-it store (str-uuid key))]
            (if (some? res) 
              (async/put! res-ch (locked-cb (prep-stream (second res))))
              (async/close! res-ch)))
          (catch Exception e (async/put! res-ch (prep-ex "Failed to retrieve binary value from store" e)))))
      res-ch))

  (-bassoc 
    ;"Doc string"
    [this key meta-up-fn input]
    (let [res-ch (async/chan 1)]
      (async/thread
        (try
          (let [old-val (get-it store (str-uuid key))
                old-meta (-deserialize serializer read-handlers (first old-val))
                new-meta (meta-up-fn old-meta)
                ^StringWriter mbaos (StringWriter.)]
            (-serialize serializer mbaos write-handlers new-meta)
            (update-it store (str-uuid key) [(.toString mbaos) input])
            (async/put! res-ch [(second old-val) input]))
          (catch Exception e (async/put! res-ch (prep-ex "Failed to update/write binary value in store" e)))))
        res-ch))

  PKeyIterable
  (-keys 
    ;"Doc string"
    [_]
    (let [res-ch (async/chan)]
      (async/thread
        (try
          (let [key-stream (get-keys store)
                keys (when key-stream
                        (for [k key-stream]
                          (:key (-deserialize serializer read-handlers (first k)))))]
            (doall
              (map #(async/put! res-ch %) keys)))
          (async/close! res-ch) 
          (catch Exception e (async/put! res-ch (prep-ex "Failed to retrieve keys from store" e)))))
        res-ch)))

; Setting up your store

(defn- store-initializer 
  "Doc string"
  [critical config]
  { :config config
    :version 1
    :auth critical
    :data (atom {})})

(defn new-your-store
  "Creates a new store connected to your backend."
  [critical-data & {:keys [config serializer read-handlers write-handlers]
                    :or   {config :default ;add the specific atom or config for your store as an object
                           serializer (ser/string-serializer)
                           read-handlers (atom {}) 
                           write-handlers (atom {})}}]
    (let [res-ch (async/chan 1)] 
      (async/thread
        (try
          ;initialize your store. 
          (let [your-conn (store-initializer critical-data config)] 
            (reality your-conn)
            (async/put! res-ch 
              (map->YourStore { :store your-conn
                                :serializer serializer
                                :read-handlers read-handlers
                                :write-handlers write-handlers
                                :locks (atom {})})))
          (catch Exception e
            (async/put! res-ch (ex-info "Could note connect to Realtime database." {:type :store-error :store critical-data  :exception e})))))
      res-ch))

(defn delete-store 
  "Doc string"
  [your-store]
  (let [res-ch (async/chan 1)]
    (async/thread
      (try
        ; do something to delete your store data.
        (reality (:store your-store))
        (update-in your-store [:store] #(dissoc % :data))
        (async/close! res-ch)
        (catch Exception e (async/put! res-ch (prep-ex "Failed to delete store" e)))))          
        res-ch))