(ns konserve-template.core
  "Address globally aggregated immutable key-value store(s)."
  (:require [clojure.core.async :refer [go]]
            [konserve.serializers :as ser]
            [hasch.core :refer [uuid]]
            [konserve.protocols :refer [PEDNAsyncKeyValueStore
                                        -exists? -get-in -update-in
                                        PBinaryAsyncKeyValueStore
                                        -bassoc -bget]]
            [clojure.core.async :as async]
            [incognito.edn :refer [read-string-safe]]))

(set! *warn-on-reflection* 1)

(defn fail [store]
  (if (nil? (:auth @store)) (throw (Exception. "Boo!")) nil)) 

(defn serialize [data]
;the simplest way to serialize data is using pr-str 
  (pr-str data))

(defn deserialize [data' read-handlers]
;and the simplest way to deserialize data is using incognito
   (read-string-safe @read-handlers data'))

(defn it-exists? [store id]
  (fail store) ;simulate store failure
  ;returns a boolean
  (some? (get-in (:data @store) [id]))) ;example
  
(defn get-it [store id read-handlers]
  (fail store) ;simulate store failure
  ;returns deserialized data as a map
  (deserialize (get-in @store [:data id]) read-handlers)) ;example

(defn update-it [store id data read-handlers]
  (fail store) ;simulate store failure
  ;1. serialize the data
  ;2. update the data
  ;3. deserialize the updated data
  ;4. return the data
  (let [serialized (serialize data)
        stored (swap! store assoc-in [:data id] serialized)] ;example
        (deserialize stored read-handlers))) ;example

(defn delete-it [store id]
  (fail store) ;simulate store failure
  ;delete the data and return nil on success
  (swap! store update-in [:data] dissoc id) ;example
  nil) 

(defrecord YourStore [store serializer read-handlers write-handlers locks]
  PEDNAsyncKeyValueStore
  (-exists? [this key] 
    (let [id (str (uuid key)) res-ch (async/chan)]
      (try 
        (let [val (it-exists? store id)]
          (async/put! res-ch val))
        (catch Exception e
          (async/put! res-ch (ex-info "Could not access item" {:type :access-error :id id :key key :exception e})))
        (finally
          (async/close! res-ch)))
      res-ch))

  (-get-in [this key-vec] 
    (let [[fkey & rkey] key-vec 
          id (str (uuid fkey))
          res-ch (async/chan)]
        (try
          (let [val (get-it store id read-handlers)]
            (if (= val nil)
              (async/close! res-ch)
              (async/put! res-ch (get-in val (into [] rkey)))))
          (catch Exception e
            (async/put! res-ch (ex-info "Could not read key." {:type :read-error :key key-vec :exception e}))))
        res-ch))

  (-update-in [this key-vec up-fn] 
    (-update-in this key-vec up-fn []))

  (-update-in [this key-vec up-fn args] 
    (let [[fkey & rkey] key-vec id (str (uuid fkey)) res-ch (async/chan)]
      (try
        (let [old (get-it store id read-handlers)
              new-data (if (empty? rkey) (apply up-fn old args) (apply update-in old rkey up-fn args))
              new (update-it store id new-data read-handlers)]
          (async/put! res-ch [(get-in old rkey) (get-in new rkey)]))
        (catch Exception e
          (async/put! res-ch (ex-info "Could not write key." {:type :write-error :key fkey :exception e})))
        (finally
          (async/close! res-ch)))
      res-ch))

  (-assoc-in [this key-vec val] 
    (-update-in this key-vec (fn [_] val)))
    
  (-dissoc [this key] 
    (let [id (str (uuid key)) res-ch (async/chan)]
        (try  
          (async/put! res-ch (delete-it store id))
          (catch Exception e
            (async/put! res-ch (ex-info "Could not delete key." {:type :write-error :key key :exception e})))
          (finally
            (async/close! res-ch)))
        res-ch)))

(defn- store-initializer [critical config]
  (atom { :config config
          :auth critical
          :data {}}))

(defn new-your-store
  "Creates an new store based on Firebase's realtime database."
  [critical-data & {:keys [config serializer read-handlers write-handlers]
                    :or   {config {:config :default} ;add the specific atom or config for your store as an object
                           serializer (ser/string-serializer) ; or (ser/fressian-serializer)  
                           read-handlers (atom {}) 
                           write-handlers (atom {})}}]
    (let [res-ch (async/chan)] 
      (try
        (let [your-conn (store-initializer critical-data config)] 
          (async/put! res-ch 
            (map->YourStore { :store your-conn
                              :error (fail your-conn) ;simulate store init error
                              :serializer serializer
                              :read-handlers read-handlers
                              :write-handlers write-handlers
                              :locks (atom {})})))
        (catch Exception e
          (async/put! res-ch (ex-info "Could note connect to Realtime database." {:type :store-error :store critical-data  :exception e}))))
      res-ch))

(defn delete-store [store]
  (reset! (:store store) nil))
