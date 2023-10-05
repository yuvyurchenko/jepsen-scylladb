(ns scylla.utils.payload-generator)

(defn generate-payload-bytes
  "Returns byte array of the given size"
  [size]
  (if (and (some? size) (pos? size))
    (byte-array (shuffle (range size)))
    nil))

(defn generate-payload-string
  "Returns randomly generated string of the given length"
  [size]
  (apply str (repeatedly size #(rand-nth "abcdefghijklmnopqrstuvwxyz0123456789"))))