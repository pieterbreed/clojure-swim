(ns swim.core)

(defn create-cluster
  "Creates a handle to a new swim cluster using id which must be unique to the cluster"
  [id]
  (list id))

(defn get-members
  "Gets a list of all of the members in the cluster"
  [cluster]
  cluster)

(defn get-id
  "Returns the id of the cluster member passed in"
  [member]
  member)

(defn foo
  "I don't do a whole lot."
  [x]
  (println x "Hello, World!"))
