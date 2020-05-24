# konserve-template

This is a starter template it give basic guide lines on how to build a konserve backend.   
The source code is heavily annotated so go check it out. 

It's important to have a solid foundation when building a library. To help you keep your quality up you can run
- `lein test` to test your code
- `lein cloverage` to see the coverage of your tests. 

A github action has been provided to get you going. 

# Status

![master](https://github.com/alekcz/konserve-template/workflows/master/badge.svg) [![codecov](https://codecov.io/gh/alekcz/konserve-template/branch/master/graph/badge.svg)](https://codecov.io/gh/alekcz/konserve-template) 

## Prerequisites

List any prerequisites for setting up you backend. 

## Usage

_Link to the your lib on clojars_

`[your/store "x.y.z"]`

```clojure
(require '[konserve-yourstore.core :refer :all]
         '[clojure.core.async :refer [<!!] :as async]
         '[konserve.core :as k])
  
  (def your-store (<!! (new-your-store connection-uri :other-config "info" :and-more :yay)))

  (<!! (k/exists? your-store  "cecilia"))
  (<!! (k/get-in your-store ["cecilia"]))
  (<!! (k/assoc-in your-store ["cecilia"] 28))
  (<!! (k/update-in your-store ["cecilia"] inc))
  (<!! (k/get-in your-store ["cecilia"]))

  (defrecord Test [a])
  (<!! (k/assoc-in your-store ["agatha"] (Test. 35)))
  (<!! (k/get-in your-store ["agatha"]))
```

## License

Copyright © 2020 Your name

This program and the accompanying materials are made available under the
terms of the Eclipse Public License 2.0 which is available at
http://www.eclipse.org/legal/epl-2.0.

This Source Code may also be made available under the following Secondary
Licenses when the conditions for such availability set forth in the Eclipse
Public License, v. 2.0 are satisfied: GNU General Public License as published by
the Free Software Foundation, either version 2 of the License, or (at your
option) any later version, with the GNU Classpath Exception which is available
at https://www.gnu.org/software/classpath/license.html.
