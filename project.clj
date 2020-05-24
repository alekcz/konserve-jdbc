(defproject alekcz/konserve-template "0.0.1-SNAPSHOT"
  :description "A YOUR-STORE backend for konserve."
  :url "https://github.com/<your handle>/konserve-<your store>"
  :license {:name "EPL-2.0 OR GPL-2.0-or-later WITH Classpath-exception-2.0"
            :url "https://www.eclipse.org/legal/epl-2.0/"}
  :aot :all
  :dependencies [[org.clojure/clojure "1.10.0"]
                 [io.replikativ/konserve "0.5.1"]
                 [io.replikativ/incognito "0.2.5"]]
  :repl-options {:init-ns konserve-template.core}
  :plugins [[lein-cloverage "1.1.2"]]
  :profiles { :dev {:dependencies [[metosin/malli "0.0.1-SNAPSHOT"]]}})
