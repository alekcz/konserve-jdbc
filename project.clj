(defproject alekcz/konserve-h2 "0.1.0-SNAPSHOT"
  :description "An H2 backend for konserve."
  :url "https://github.com/alekcz/konserve-h2"
  :license {:name "EPL-2.0 OR GPL-2.0-or-later WITH Classpath-exception-2.0"
            :url "https://www.eclipse.org/legal/epl-2.0/"}
  :aot :all
  :dependencies [[org.clojure/clojure "1.10.0"]
                 [com.h2database/h2 "1.4.200"]
                 [org.clojure/java.jdbc "0.7.11"]
                 [io.replikativ/konserve "0.6.0-20200512.093105-1"]]
  :repl-options {:init-ns konserve-h2.core}
  :plugins [[lein-cloverage "1.1.3-SNAPSHOT"]]
  :profiles { :dev {:dependencies [[metosin/malli "0.0.1-SNAPSHOT"]]}})
