(defproject alekcz/konserve-jdbc "0.2.0-SNAPSHOT"
  :description "A generic JDBC backend for konserve."
  :url "https://github.com/alekcz/konserve-jdbc"
  :license {:name "EPL-2.0 OR GPL-2.0-or-later WITH Classpath-exception-2.0"
            :url "https://www.eclipse.org/legal/epl-2.0/"}
  :dependencies [[org.clojure/clojure "1.11.1"]
                 [com.h2database/h2 "2.1.214"]
                 [com.github.seancorfield/next.jdbc "1.3.862"]
                 [org.apache.derby/derby "10.16.1.1"]
                 [com.mysql/mysql-connector-j "8.0.32"]
                 [org.postgresql/postgresql "42.6.0"]
                 [org.xerial/sqlite-jdbc "3.41.2.0"]
                 [io.replikativ/konserve "0.6.0-alpha3"]
                 [com.microsoft.sqlserver/mssql-jdbc "9.1.1.jre8-preview"]
                 [com.mchange/c3p0 "0.9.5.5"]]
  :javac-options ["--release" "8" "-g"]
  :repl-options {:init-ns konserve-jdbc.core}
  :plugins [[lein-cloverage "1.2.4"]]
  :profiles { :dev {:dependencies [[metosin/malli "0.8.9"]]}})
