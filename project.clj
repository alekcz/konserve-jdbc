(defproject alekcz/konserve-template "0.0.1-SNAPSHOT"
  :description "A YOUR-STORE backend for konserve."
  :url "https://github.com/<your handle>/konserve-<your store>"
  :license {:name "EPL-2.0 OR GPL-2.0-or-later WITH Classpath-exception-2.0"
            :url "https://www.eclipse.org/legal/epl-2.0/"}
  :aot :all
  :dependencies [[org.clojure/clojure "1.10.0"]
                 ;[io.replikativ/konserve "0.6.0-SNAPSHOT"]
                 ]
  :repl-options {:init-ns konserve-template.core}
  :plugins [[lein-cloverage "1.1.3-SNAPSHOT"] [lein-git-deps "0.0.2"]]
  :git-dependencies [["https://github.com/replikativ/konserve.git" "4fac6a9566b2402122f3664aac74d3a08eb36eb3"]]
  :profiles { :dev {:dependencies [[metosin/malli "0.0.1-SNAPSHOT"]]}})
