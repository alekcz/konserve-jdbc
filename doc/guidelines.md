# Implementing your conserve backend

This guide aims to help you implement konserve on top of a backend store of your choice. . It assumes that you're more or less familiar with clojure and the backend store you wish to integrate with. We'll not unpack the finer points of konserve but rather what you need to know to implement you store successfully. You can checkout the konserve repo for a [more detailed explanation](https://github.com/replikativ/konserve/blob/feature_metadata_support/doc/backend.org).

## Unpacking konserve
The idea of konserve is to abstract away the implementation differences across backend stores or databases and allow them all to be interacted with as if they are clojure maps. As a result the konserve API provides the following core functions:

- `exists?`
- `get` 
- `get-in`
- `assoc`
- `assoc-in`
- `update`
- `update-in`
- `dissoc`
- `bget`
- `bassoc`
- `keys`

These functions are conceptually identical to their clojure counterparts (with bget and bassoc) dealing with binary data. 

There are a few other functions in the API that are used internally for data integrity and performance but aren't particulary relevent at to us at this point. 

### Data representation in flight
Data moves through konserve as tuples containing the meta data as well as the actual data to be stored like so `[meta data]`. 
So if you see (first x) that is most probably meta data. And if you see (second x) it's probably that actual data.

### Data representation at rest
Because you have no control of what your users of will decide to store in your backend, the data needs to be serialized in a robust manner. To help you with this konserve ships with two serializers a string serializer and a fressian serializer. Both serializers use [incognito](https://github.com/replikativ/incognito) to enable the end user to serialize custom clojure types in your store. 

### Strings, binary data and multiple locations
Binary stores are substantially more storage efficient than string stores, but add complexity. 

Writing a konserve tuple to a binary store generally happens as follows:
1. Meta data is serialized.
2. Length of the data is computed and store in a byte-array of 8 bytes (`ByteBuffer` simplifies this). 
3. The data is serialized. 
4. The final data is a concatenation of the length of the metadata, then the metadata, then finally the serialized data. 

```clojure
(byte-array
  (into []
    (concat [meta-length] [serialized-metadata] [serialized-metada])))
 ```

Reading from a binary store then generally happens as follows
1. Binary data is retrieved
2. The first 8 bytes are read and converted to an integer, n.
3. The next n bytes are read and deserialzed to retrieve the metadata
4. The data is the deserialized, starting from the end fo the metadata to the end of the byte-array or stream
5. And so you have your konserve tuple. 

In this repo [binary.clj](../src/konserve_template/binary.clj) gives an example implementation of this. 

Where possible, it's best to store you metadata in a way that allows it to be retrieved separately. 
This should only be done if writing to two separate locations can be done atomically. This saves you the hassle slicing a byte-array and deally with portions of it. 

In this repo [string.clj](../src/konserve_template/string.clj) gives an example of a store that only accepts strings.

### I/O
All IO operation in conserve are asynchronouns and use [core.async](https://github.com/clojure/core.async). To help you avoid pitfalls in this regards this repo has simulates a backend store with inconsistent latency and implements the structure to handle that asynchronously. 

You'll notice that every interaction with your store immediately returns a channel then starts the IO in a separate thread. Data and exceptions are both returned into this channel. And if the result of an operation is `nil` you simply close the channel (putting `nil` into a channel is not permitted). If your store is blazingly fast (i.e. in memory) you can use go-blocks instead, but keep in mind that go-blocks are not threads and so when many IO interactions are triggered everything can lock up. 


### Version 0.6.0 of the konserve protocol
To support the core konserve API you need to implement the following methods as described in the konserve protocol: 
```clojure
(-exists? [this key] "Checks whether value is in the store.")
(-get-meta [this key] "Fetch only metadata for the key.")
(-get [this key] "Returns the value stored described by key or nil if the path is not resolvable.")
(-update-in [this key-vec meta-up-fn up-fn up-fn-args]
    "Updates a position described by key-vec by applying up-fn and storing the result atomically.    
    Returns a vector [old new] of the previous value and the result of applying up-fn 
    (the newly stored value).    
    meta-up-fn is an internal konserve function that updates the metadata of the store")
(-assoc-in [this key-vec meta-up-fn val]) 
(-dissoc [this key]) 
(-keys [this] "Return a channel that will continuously yield keys in this store.")
; optional methods
(-bget [this key locked-cb] "Calls locked-cb with a platform specific binary representation inside the lock,   
    e.g. wrapped InputStream on the JVM and Blob in JavaScript. You need to properly close/dispose 
    the object when you are done!")
(-bassoc [this key meta-up-fn val] "Copies given value (InputStream, Reader, File, byte[] or String on JVM, 
    Blob in JavaScript) under key in the store.")
```
