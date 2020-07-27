(ns konserve-jdbc.core-h2-test
  (:require [clojure.test :refer [deftest is testing use-fixtures]]
            [clojure.core.async :refer [<!!] :as async]
            [konserve.core :as k]
            [konserve-jdbc.core :refer [new-jdbc-store delete-store] :as kjc]
            [hasch.core :as hasch]
            [malli.generator :as mg]
            [clojure.java.jdbc :as j]
            [clojure.java.io :as io])
  (:import  [clojure.lang ExceptionInfo]
            [java.sql Blob]
            [java.io File]))


(defn delete-recursively [fname]
  (let [func (fn [func f]
               (when (.isDirectory ^File f)
                 (doseq [^File f2 (.listFiles ^File f)]
                   (func func f2)))
               (try (io/delete-file f) (catch Exception _ nil)))]
    (func func (io/file fname))))

(defn my-test-fixture [f]
  (f)
  (delete-recursively "./temp"))

(use-fixtures :once my-test-fixture)

(def conn 
  { :dbtype "h2"
    :dbname "./temp/konserve;DB_CLOSE_ON_EXIT=FALSE"
    :user "sa"
    :password ""
   })

(deftest get-nil-test
  (testing "Test getting on empty store"
    (let [_ (println "Getting from an empty store")
          store (<!! (new-jdbc-store conn :table "nil"))]
      (is (= nil (<!! (k/get store :foo))))
      (is (= nil (<!! (k/get-meta store :foo))))
      (is (not (<!! (k/exists? store :foo))))
      (is (= :default (<!! (k/get-in store [:fuu] :default))))
      (<!! (k/bget store :foo (fn [res] 
                                (is (nil? res)))))
      (delete-store store))))

(deftest write-value-test
  (testing "Test writing to store"
    (let [_ (println "Writing to store")
          store (<!! (new-jdbc-store conn :table "test_write"))]
      (is (not (<!! (k/exists? store :foo))))
      (<!! (k/assoc store :foo :bar))
      (is (<!! (k/exists? store :foo)))
      (is (= :bar (<!! (k/get store :foo))))
      (is (= :foo (:key (<!! (k/get-meta store :foo)))))
      (<!! (k/assoc-in store [:baz] {:bar 42}))
      (is (= 42 (<!! (k/get-in store [:baz :bar]))))
      (delete-store store))))

(deftest update-value-test
  (testing "Test updating values in the store"
    (let [_ (println "Updating values in the store")
          store (<!! (new-jdbc-store conn :table "test_update"))]
      (<!! (k/assoc store :foo :baritone))
      (is (= :baritone (<!! (k/get-in store [:foo]))))
      (<!! (k/update-in store [:foo] name))
      (is (= "baritone" (<!! (k/get-in store [:foo]))))
      (delete-store store))))

(deftest exists-test
  (testing "Test check for existing key in the store"
    (let [_ (println "Checking if keys exist")
          store (<!! (new-jdbc-store conn :table "test_exists"))]
      (is (not (<!! (k/exists? store :foo))))
      (<!! (k/assoc store :foo :baritone))
      (is  (<!! (k/exists? store :foo)))
      (<!! (k/dissoc store :foo))
      (is (not (<!! (k/exists? store :foo))))
      (delete-store store))))

(deftest binary-test
  (testing "Test writing binary date"
    (let [_ (println "Reading and writing binary data")
          store (<!! (new-jdbc-store conn :table "test_binary"))
          cb (atom false)
          cb2 (atom false)]
      (is (not (<!! (k/exists? store :binbar))))
      (<!! (k/bget store :binbar (fn [ans] (is (nil? (:input-stream ans))))))
      (<!! (k/bassoc store :binbar (byte-array (range 10))))
      (<!! (k/bget store :binbar (fn [res]
                                    (reset! cb true)
                                    (is (= (map byte (slurp (:input-stream res)))
                                           (range 10))))))
      (<!! (k/bassoc store :binbar (byte-array (map inc (range 10))))) 
      (<!! (k/bget store :binbar (fn [res]
                                    (reset! cb2 true)
                                    (is (= (map byte (slurp (:input-stream res)))
                                           (map inc (range 10)))))))                                          
      (is (<!! (k/exists? store :binbar)))
      (is @cb)
      (is @cb2)
      (delete-store store))))
  
(deftest key-test
  (testing "Test getting keys from the store"
    (let [_ (println "Getting keys from store")
          store (<!! (new-jdbc-store conn :table "test_key"))]
      (is (= #{} (<!! (async/into #{} (k/keys store)))))
      (<!! (k/assoc store :baz 20))
      (<!! (k/assoc store :binbar 20))
      (is (= #{:baz :binbar} (<!! (async/into #{} (k/keys store)))))
      (delete-store store))))  

(deftest append-test
  (testing "Test the append store functionality."
    (let [_ (println "Appending to store")
          store (<!! (new-jdbc-store conn :table "test_append"))]
      (<!! (k/append store :foo {:bar 42}))
      (<!! (k/append store :foo {:bar 43}))
      (is (= (<!! (k/log store :foo))
             '({:bar 42}{:bar 43})))
      (is (= (<!! (k/reduce-log store
                              :foo
                              (fn [acc elem]
                                (conj acc elem))
                              []))
             [{:bar 42} {:bar 43}]))
      (delete-store store))))

(def home
  [:map
    [:name string?]
    [:description string?]
    [:rooms pos-int?]
    [:capacity float?]
    [:address
      [:map
        [:street string?]
        [:number int?]
        [:country [:enum "kenya" "lesotho" "south-africa" "italy" "mozambique" "spain" "india" "brazil" "usa" "germany"]]]]])

(deftest realistic-test
  (testing "Realistic data test."
    (let [_ (println "Entering realistic data")
          store (<!! (new-jdbc-store conn :table "test_realistic"))
          home (mg/generate home {:size 20 :seed 2})
          address (:address home)
          addressless (dissoc home :address)
          name (mg/generate keyword? {:size 15 :seed 3})
          num1 (mg/generate pos-int? {:size 5 :seed 4})
          num2 (mg/generate pos-int? {:size 5 :seed 5})
          floater (mg/generate float? {:size 5 :seed 6})]
      
      (<!! (k/assoc store name addressless))
      (is (= addressless 
             (<!! (k/get store name))))

      (<!! (k/assoc-in store [name :address] address))
      (is (= home 
             (<!! (k/get store name))))

      (<!! (k/update-in store [name :capacity] * floater))
      (is (= (* floater (:capacity home)) 
             (<!! (k/get-in store [name :capacity]))))  

      (<!! (k/update-in store [name :address :number] + num1 num2))
      (is (= (+ num1 num2 (:number address)) 
             (<!! (k/get-in store [name :address :number]))))             
      
      (delete-store store))))   

(deftest bulk-test
  (testing "Bulk data test."
    (let [_ (println "Writing bulk data")
          store (<!! (new-jdbc-store conn :table "test_bulk"))
          string20MB (apply str (vec (range 3000000)))
          range2MB 2097152
          sevens (repeat range2MB 7)]
      (print "\nWriting 20MB string: ")
      (time (<!! (k/assoc store :record string20MB)))
      (is (= (count string20MB) (count (<!! (k/get store :record)))))
      (print "Writing 2MB binary: ")
      (time (<!! (k/bassoc store :binary (byte-array sevens))))
      (<!! (k/bget store :binary (fn [{:keys [input-stream]}]
                                    (is (= (pmap byte (slurp input-stream))
                                           sevens)))))
      (delete-store store))))  

(deftest version-test
  (testing "Test check for store version being store with data"
    (let [_ (println "Checking if store version is stored")
          store (<!! (new-jdbc-store conn :table "test_version"))
          id (str (hasch/uuid :foo))]
      (<!! (k/assoc store :foo :bar))
      (is (= :bar (<!! (k/get store :foo))))
      (is (= (byte kjc/store-version) 
             (j/with-db-connection [db (-> store :conn :db)]
                (let [res (first (j/query db [(str "select id,meta from " (-> store :conn :table) " where id = '" id "'")]))
                      ^Blob meta (:meta res)]
                  (-> (.getBytes meta 0 (.length meta)) vec (nth 0) )))))
      (is (= (byte kjc/store-version) 
             (j/with-db-connection [db (-> store :conn :db)]
                (let [res (first (j/query db [(str "select id,data from " (-> store :conn :table) " where id = '" id "'")]))
                      ^Blob data (:data res)]
                  (-> (.getBytes data 0 (.length data)) vec (nth 0) )))))           
      (delete-store store))))

(deftest serializer-test
  (testing "Test check for serilizer type being store with data"
    (let [_ (println "Checking if serilizer type is stored")
          store (<!! (new-jdbc-store conn :table "test_serializer"))
          id (str (hasch/uuid :foo))]
      (<!! (k/assoc store :foo :bar))
      (is (= :bar (<!! (k/get store :foo))))
      (is (= (byte kjc/serializer) 
             (j/with-db-connection [db (-> store :conn :db)]
                (let [res (first (j/query db [(str "select id,meta from " (-> store :conn :table) " where id = '" id "'")]))
                      ^Blob meta (:meta res)]
                  (-> (.getBytes meta 0 (.length meta)) vec (nth 1) )))))
      (is (= (byte kjc/serializer) 
             (j/with-db-connection [db (-> store :conn :db)]
                (let [res (first (j/query db [(str "select id,data from " (-> store :conn :table) " where id = '" id "'")]))
                      ^Blob data (:data res)]
                  (-> (.getBytes data 0 (.length data)) vec (nth 1) )))))           
      (delete-store store))))

(deftest compressor-test
  (testing "Test check for compressor type being store with data"
    (let [_ (println "Checking if compressor type is stored")
          store (<!! (new-jdbc-store conn :table "test_compressor"))
          id (str (hasch/uuid :foo))]
      (<!! (k/assoc store :foo :bar))
      (is (= :bar (<!! (k/get store :foo))))
      (is (= (byte kjc/compressor) 
             (j/with-db-connection [db (-> store :conn :db)]
                (let [res (first (j/query db [(str "select id,meta from " (-> store :conn :table) " where id = '" id "'")]))
                      ^Blob meta (:meta res)]
                  (-> (.getBytes meta 0 (.length meta)) vec (nth 2) )))))
      (is (= (byte kjc/compressor) 
             (j/with-db-connection [db (-> store :conn :db)]
                (let [res (first (j/query db [(str "select id,data from " (-> store :conn :table) " where id = '" id "'")]))
                      ^Blob data (:data res)]
                  (-> (.getBytes data 0 (.length data)) vec (nth 2) )))))           
      (delete-store store))))

(deftest encryptor-test
  (testing "Test check for encryptor type being store with data"
    (let [_ (println "Checking if encryptor type is stored")
          store (<!! (new-jdbc-store conn :table "test_encryptor"))
          id (str (hasch/uuid :foo))]
      (<!! (k/assoc store :foo :bar))
      (is (= :bar (<!! (k/get store :foo))))
      (is (= (byte kjc/encryptor) 
             (j/with-db-connection [db (-> store :conn :db)]
                (let [res (first (j/query db [(str "select id,meta from " (-> store :conn :table) " where id = '" id "'")]))
                      ^Blob meta (:meta res)]
                  (-> (.getBytes meta 0 (.length meta)) vec (nth 3) )))))
      (is (= (byte kjc/encryptor) 
             (j/with-db-connection [db (-> store :conn :db)]
                (let [res (first (j/query db [(str "select id,data from " (-> store :conn :table) " where id = '" id "'")]))
                      ^Blob data (:data res)]
                  (-> (.getBytes data 0 (.length data)) vec (nth 3) )))))           
      (delete-store store))))            

(deftest exceptions-test
  (testing "Test exception handling"
    (let [_ (println "Generating exceptions")
          store (<!! (new-jdbc-store conn :table "test_exceptions"))
          corrupt (update-in store [:conn] #(dissoc % :db :ds))] ; let's corrupt our store
      (is (= ExceptionInfo (type (<!! (new-jdbc-store conn :table "")))))
      (is (= ExceptionInfo (type (<!! (k/get corrupt :bad)))))
      (is (= ExceptionInfo (type (<!! (k/get-meta corrupt :bad)))))
      (is (= ExceptionInfo (type (<!! (k/assoc corrupt :bad 10)))))
      (is (= ExceptionInfo (type (<!! (k/dissoc corrupt :bad)))))
      (is (= ExceptionInfo (type (<!! (k/assoc-in corrupt [:bad :robot] 10)))))
      (is (= ExceptionInfo (type (<!! (k/update-in corrupt [:bad :robot] inc)))))
      (is (= ExceptionInfo (type (<!! (k/exists? corrupt :bad)))))
      (is (= ExceptionInfo (type (<!! (k/keys corrupt)))))
      (is (= ExceptionInfo (type (<!! (k/bget corrupt :bad (fn [_] nil))))))   
      (is (= ExceptionInfo (type (<!! (k/bassoc corrupt :binbar (byte-array (range 10)))))))   
      (is (= ExceptionInfo (type (<!! (delete-store corrupt)))))
      (delete-store store))))
