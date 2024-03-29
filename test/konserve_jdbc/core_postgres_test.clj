(ns konserve-jdbc.core-postgres-test
  (:require [clojure.test :refer [deftest is testing use-fixtures]]
            [clojure.core.async :refer [<!!] :as async]
            [konserve.core :as k]
            [konserve.storage-layout :as kl]
            [konserve-jdbc.core :refer [new-jdbc-store delete-store]]
            [konserve.compressor :as comp]
            [malli.generator :as mg]
            [next.jdbc :as jdbc]))

(def conn { :dbtype "postgresql"
            :dbname "konserve"
            :host "localhost"
            :user "konserve"
            :password "password"
            :initialPoolSize 25
            :numHelperThreads 25
            :minPoolSize 20
            :maxPoolSize 50})
(def conn2 {:jdbcUrl "postgres://konserve:password@localhost/konserve"})
(def conn3 {:jdbcUrl "postgres://konserve:password@localhost:5432/konserve"
            :user "fakeuser"
            :password "fakepassword"})
(def conn-binary-test {:jdbcUrl "postgres://localhost/konserve"
                       :user "konserve"
                       :password "password"})

(deftype UnknownType [])

(defn exception? [thing]
  (instance? Throwable thing))

(defn reset-db [f]
  (f)
  (with-open [con (jdbc/get-connection conn)]
    (jdbc/execute! con ["drop table if exists konserve_default"])
    (jdbc/execute! con ["drop table if exists konserve_test_write"])
    (jdbc/execute! con ["drop table if exists konserve_test_update"])
    (jdbc/execute! con ["drop table if exists konserve_test_exists"])
    (jdbc/execute! con ["drop table if exists konserve_test_binary"])
    (jdbc/execute! con ["drop table if exists konserve_test_key"])
    (jdbc/execute! con ["drop table if exists konserve_test_append"])
    (jdbc/execute! con ["drop table if exists konserve_test_realistic"])
    (jdbc/execute! con ["drop table if exists konserve_test_bulk"])
    (jdbc/execute! con ["drop table if exists konserve_test_headers"])
    (jdbc/execute! con ["drop table if exists konserve_test_values"])
    (jdbc/execute! con ["drop table if exists konserve_test_exceptions"])))

(use-fixtures :once reset-db)

(deftest get-nil-test
  (testing "Test getting on empty store"
    (let [_ (println "Getting from an empty store")
          store (<!! (new-jdbc-store conn :table "default" :debug true))]
      (is (= nil (<!! (k/get store :foo))))
      (is (= nil (<!! (k/get-meta store :foo))))
      (is (not (<!! (k/exists? store :foo))))
      (is (= :default (<!! (k/get-in store [:fuu] :default))))
      (<!! (k/bget store :foo (fn [res] 
                                (is (nil? res))))))))

(deftest write-value-test
  (testing "Test writing to store"
    (let [_ (println "Writing to store")
          store (<!! (new-jdbc-store (assoc conn :table "test_write" :debug true)))]
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
          store (<!! (new-jdbc-store conn2 :table "test_update"))]
      (<!! (k/assoc store :foo :baritone))
      (is (= :baritone (<!! (k/get-in store [:foo]))))
      (<!! (k/update-in store [:foo] name))
      (is (= "baritone" (<!! (k/get-in store [:foo]))))
      (delete-store store))))

(deftest exists-test
  (testing "Test check for existing key in the store"
    (let [_ (println "Checking if keys exist")
          store (<!! (new-jdbc-store conn3 :table "test_exists"))]
      (is (not (<!! (k/exists? store :foo))))
      (<!! (k/assoc store :foo :baritone))
      (is  (<!! (k/exists? store :foo)))
      (<!! (k/dissoc store :foo))
      (is (not (<!! (k/exists? store :foo))))
      (delete-store store))))

(deftest binary-test
  (testing "Test writing binary data"
    (let [_ (println "Reading and writing binary data")
          store (<!! (new-jdbc-store conn-binary-test :table "test_binary"))
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

(deftest perf-test
  (testing "Insert perf test."
    (let [num 1000
          _ (println (str "Writing " num " bits of data"))
          store (<!! (new-jdbc-store conn :table "test_perf"))
          range20K 20000
          sevens (apply str (vec (repeat range20K 7)))]
      (print (str "Writing 20KB x " num ": "))
      (time 
        (doseq [n (range num)]
          (<!! (k/assoc store (keyword (str "num-" n)) sevens))))
      (doseq [n (range num)]
          (is (= (count sevens) (count (<!! (k/get store (keyword (str "num-" n))))))))
      (delete-store store))))  

(deftest limit-test
  (testing "Rows returned limit test."
    (let [num 2000
          _ (println (str "Writing " num " bits of data"))
          store (<!! (new-jdbc-store conn :table "test_limit"))
          data {:random "map"}]
      (time 
        (doseq [n (range num)]
          (<!! (k/assoc store (keyword (str "num-" n)) data))))
      (is (= num (count (<!! (async/into #{} (k/keys store))))))
      (delete-store store))))  

(deftest raw-meta-test
  (testing "Test header storage"
    (let [_ (println "Checking if headers are stored correctly")
          store (<!! (new-jdbc-store conn :table "test_headers"))]
      (<!! (k/assoc store :foo :bar))
      (<!! (k/assoc store :eye :ear))
      (let [mraw (<!! (kl/-get-raw-meta store :foo))
            mraw2 (<!! (kl/-get-raw-meta store :eye))
            mraw3 (<!! (kl/-get-raw-meta store :not-there))
            header (take 4 (map byte mraw))]
        (<!! (kl/-put-raw-meta store :foo mraw2))
        (<!! (kl/-put-raw-meta store :baritone mraw2))
        (is (= header [1 1 0 0]))
        (is (nil? mraw3))
        (is (= :eye (:key (<!! (k/get-meta store :foo)))))
        (is (= :eye (:key (<!! (k/get-meta store :baritone))))))        
      (delete-store store))))          

(deftest raw-value-test
  (testing "Test value storage"
    (let [_ (println "Checking if values are stored correctly")
          store (<!! (new-jdbc-store (assoc conn :compressor comp/lz4-compressor) :table "test_values"))]
      (<!! (k/assoc store :foo :bar))
      (<!! (k/assoc store :eye :ear))
      (let [vraw (<!! (kl/-get-raw-value store :foo))
            vraw2 (<!! (kl/-get-raw-value store :eye))
            vraw3 (<!! (kl/-get-raw-value store :not-there))
            header (take 4 (map byte vraw))]
        (<!! (kl/-put-raw-value store :foo vraw2))
        (<!! (kl/-put-raw-value store :baritone vraw2))
        (is (= header [1 1 1 0]))
        (is (nil? vraw3))
        (is (= :ear (<!! (k/get store :foo))))
        (is (= :ear (<!! (k/get store :baritone)))))      
      (delete-store store))))   

(deftest exceptions-test
  (testing "Test exception handling"
    (let [_ (println "Generating exceptions")
          store (<!! (new-jdbc-store conn :table "test_exceptions"))
          params (clojure.core/keys store)
          corruptor (fn [s k] 
                        (if (= (type (k s)) clojure.lang.Atom)
                          (clojure.core/assoc-in s [k] (atom {})) 
                          (clojure.core/assoc-in s [k] (UnknownType.))))
          corrupt (reduce corruptor store params)] ; let's corrupt our store
      (is (exception? (<!! (new-jdbc-store {} :table "test_exceptions"))))
      (is (exception? (<!! (k/get corrupt :bad))))
      (is (exception? (<!! (k/get-meta corrupt :bad))))
      (is (exception? (<!! (k/assoc corrupt :bad 10))))
      (is (exception? (<!! (k/dissoc corrupt :bad))))
      (is (exception? (<!! (k/assoc-in corrupt [:bad :robot] 10))))
      (is (exception? (<!! (k/update-in corrupt [:bad :robot] inc))))
      (is (exception? (<!! (k/exists? corrupt :bad))))
      (is (exception? (<!! (k/keys corrupt))))
      (is (exception? (<!! (k/bget corrupt :bad (fn [_] nil)))))   
      (is (exception? (<!! (k/bassoc corrupt :binbar (byte-array (range 10))))))
      (is (exception? (<!! (delete-store corrupt)))))))
