# konserve-jdbc

A [JDBC](https://github.com/clojure/java.jdbc) backend for [konserve](https://github.com/replikativ/konserve). 

## Status

![master](https://github.com/alekcz/konserve-jdbc/workflows/master/badge.svg) [![codecov](https://codecov.io/gh/alekcz/konserve-jdbc/branch/master/graph/badge.svg)](https://codecov.io/gh/alekcz/konserve-jdbc) [![Dependencies Status](https://versions.deps.co/alekcz/konserve-jdbc/status.svg)](https://versions.deps.co/alekcz/konserve-jdbc)

## Usage

[![Clojars Project](https://img.shields.io/clojars/v/alekcz/konserve-jdbc.svg)](https://clojars.org/alekcz/konserve-jdbc)

`[alekcz/konserve-jdbc "0.1.0-SNAPSHOT"]`

```clojure
(require '[konserve-jdbc.core :refer [new-jdbc-store]]
         '[clojure.core.async :refer [<!!] :as async]
         '[konserve.core :as k])
  
  (def h2 { :dbtype "h2"
            :dbname "./temp/konserve;DB_CLOSE_ON_EXIT=FALSE"
            :user "sa"
            :password ""})
  
  (def mysql { :dbtype "mysql"
               :dbname "konserve"
               :host "localhost"
               :user "konserve"
               :password "password"})

  (def pg { :dbtype "postgresql"
            :dbname "konserve"
            :host "localhost"
            :user "konserve"
            :password "password"})

  (def h2-store (<!! (new-jdbc-store h2 :table "konserve")))
  (def mysql-store (<!! (new-jdbc-store mysql :table "konserve")))
  (def pg-store (<!! (new-jdbc-store pg :table "konserve")))

  (<!! (k/exists? pg-store  "cecilia"))
  (<!! (k/get-in pg-store ["cecilia"]))
  (<!! (k/assoc-in pg-store ["cecilia"] 28))
  (<!! (k/update-in pg-store ["cecilia"] inc))
  (<!! (k/get-in pg-store ["cecilia"]))

  (defrecord Test [a])
  (<!! (k/assoc-in pg-store ["agatha"] (Test. 35)))
  (<!! (k/get-in pg-store ["agatha"]))
```

## License

Copyright © 2020 Alexander Oloo

This program and the accompanying materials are made available under the
terms of the Eclipse Public License 2.0 which is available at
http://www.eclipse.org/legal/epl-2.0.

This Source Code may also be made available under the following Secondary
Licenses when the conditions for such availability set forth in the Eclipse
Public License, v. 2.0 are satisfied: GNU General Public License as published by
the Free Software Foundation, either version 2 of the License, or (at your
option) any later version, with the GNU Classpath Exception which is available
at https://www.gnu.org/software/classpath/license.html.
