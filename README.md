# konserve-template

An [H2](https://www.h2database.com/html/main.html) backend for [konserve](https://github.com/replikativ/konserve). 

## Status

![master](https://github.com/alekcz/konserve-h2/workflows/master/badge.svg) [![codecov](https://codecov.io/gh/alekcz/konserve-h2/branch/master/graph/badge.svg)](https://codecov.io/gh/alekcz/konserve-h2) 

## Usage

[![Clojars Project](https://img.shields.io/clojars/v/alekcz/konserve-h2.svg)](https://clojars.org/alekcz/konserve-h2)

`[alekcz/konserve-h2 "0.1.0-SNAPSHOT"]`

```clojure
(require '[konserve-h2.core :refer [new-your-store]]
         '[clojure.core.async :refer [<!!] :as async]
         '[konserve.core :as k])
  
  (def h2-store (<!! (new-h2-store "./temp/db" :table "konserve")))

  (<!! (k/exists? h2-store  "cecilia"))
  (<!! (k/get-in h2-store ["cecilia"]))
  (<!! (k/assoc-in h2-store ["cecilia"] 28))
  (<!! (k/update-in h2-store ["cecilia"] inc))
  (<!! (k/get-in h2-store ["cecilia"]))

  (defrecord Test [a])
  (<!! (k/assoc-in h2-store ["agatha"] (Test. 35)))
  (<!! (k/get-in h2-store ["agatha"]))
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
