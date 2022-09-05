(defproject alekcz/konserve-jdbc "0.2.0-SNAPSHOT"
  :description "A generic JDBC backend for konserve."
  :url "https://github.com/alekcz/konserve-jdbc"
  :license {:name "EPL-2.0 OR GPL-2.0-or-later WITH Classpath-exception-2.0"
            :url "https://www.eclipse.org/legal/epl-2.0/"}
  :dependencies [[org.clojure/clojure "1.11.1"]
                 [com.h2database/h2 "1.4.200"]
                 [seancorfield/next.jdbc "1.2.659"]
                 [org.apache.derby/derby "10.14.2.0"]
                 [mysql/mysql-connector-java "8.0.20"]
                 [org.postgresql/postgresql "42.2.12"]
                 [org.xerial/sqlite-jdbc "3.23.1"]
                 [io.replikativ/konserve "0.6.0-alpha3"]
                 [com.microsoft.sqlserver/mssql-jdbc "9.1.1.jre8-preview"]]
  :javac-options ["--release" "8" "-g"]
  :repl-options {:init-ns konserve-jdbc.core}
  :plugins [[lein-cloverage "1.2.4"]]
  :profiles { :dev {:dependencies [[metosin/malli "0.5.1"]]}})
