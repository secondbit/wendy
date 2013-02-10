This document exists to offer a plain-English explanation of how Wendy works. Wendy is based heavily on [Pastry](http://research.microsoft.com/en-us/um/people/antr/PAST/pastry.pdf), but there is no guarantee that Wendy does not stray from the Pastry paper. This document is the only canon spec for how Wendy functions.

## Metrics

### Node and Message IDs

IDs are simply 128 bits that can be used to uniquely identify a message or node. Note that node and message IDs are in the same format. They can be visualised as a single circle, like a clock; at the top of the circle is 0, and it counts up until it gets to (2^128)-1 (which is the maximum possible value an ID can hold), which sets next to 0. Thus the number line loops around, ensuring that there is always an ID which is modularly greater than the current ID and an ID that is modularly less than the current ID.

IDs are the only metric used when actually routing a Message, as will be explained.

### Proximity

Each node in the cluster keeps a "proximity metric" for every other node it knows about in the cluster. The proximity metric is simply a measurement of how close in the network topology one node is to another. Currently, this is measured by the time a single request takes between two nodes, and is updated every time the nodes communicate. For performance reasons, there is also a basic cache of proximity scores for nodes (both known and unknown) that the current node has encountered. This cache empties itself every hour, but is unbounded in the memory it can consume.

Proximity is only used when populating state tables. The proximity is never used during routing itself.

## State Tables

Wendy maintains three state tables, arrays of known nodes in the cluster that are used either for routing or maintaining the other state tables.

### Routing Table

The routing table is a two-dimensional array, consisting of 32 rows and sixteen columns each. Each column is capable of holding a single node. The routing table exists to keep a representational portion of the cluster, for the purposes of routing messages.

The routing table is populated by dividing the ID of a node into 32 digits, each with 16 possible values. To determine which row a node belongs in, the common prefix is calculated between the current node and the node being inserted. For example, if the current node has an ID of `1A2BC3D..` and the inserted node has an ID of `1A2BC3E..`, the common prefix is `1A2BC3`. The length of this common prefix is the row the node will be inserted into in the routing table. To determine which column a node belongs in, take the value of the of the first different digit in the ID (`E` in our example) as a base 16 number (15). So our example node would be inserted into row 6, column 15 of the routing table.

When a node leaves the cluster, the routing table is repaired by finding another suitable node to take its place. Assume the node in row 6, column 15 of the routing table has failed. To find another suitable node, the other nodes in the row are asked for the node at that position in _their_ routing tables. Because those nodes all share the same prefix length, any node they have in that position would be appropriate for the current node's empty position. If there are no known nodes in the same row, or if none of them have a node at the empty position, then the process is repeated on the next row in the routing table (row 7, in our example). Because each node in this row has a common prefix greater than the common prefix of our empty position, they too will all have an appropriate value for our empty position. This is repeated until the end of the routing table is reached. If a node exists that is a suitable replacement, this process is highly likely to find it.

When two nodes are both equally suited to fill a position in the routing table, the neighborhood set is consulted to determine which node has a closer proximity in the network topology to the current node. This ensures that routing has good locality properties and favours nodes that will take less time to communicate.

### Leaf Set

The leaf set can be visualised as two arrays of 16 nodes each. The leaf set exists to keep a list of the immediate neighbours in the node ID space for the current node, for the purposes of routing. One array of 16 nodes (the "left" array) contains nodes that have lower IDs than the current node's ID. The other (the "right" array) contains nodes that have greater IDs than the current node's ID.

The leaf set is populated by determining whether the inserted node's ID is greater than or less than the current node's ID. Once this is determined, the appropriate array is selected. Each node in the array is checked against the inserted node's ID. If the ID falls between the current node's ID and the ID of the node being checked, the inserted node is inserted at that location in the leaf set, and the rest of the nodes in the leaf set are pushed back by a single position. If during this check, an unfilled position is encountered in the leaf set, the inserted node assumes that position. After this process is done, the leaf set is limited to the 16 nodes with IDs closest to the current node's ID. This allows the leaf set to consist of an array of the 16 nodes whose IDs are the closest to the current node's while being greater, and 16 nodes whose IDs are the closest to the current node's while being lesser.

The leaf set is repaired by choosing the furthest node on the same side as the removed node, asking for its leaf set, and recalculating the array based on that information. Unless all 16 nodes on a single side of the leaf set depart the cluster before the leaf set can be repaired, this process is guaranteed to keep the leaf set repaired.

### Neighborhood Set

The neighborhood set is simply an array of 32 nodes. It exists to keep a list of the nodes that are closest to the current node in the network topology, ensuring that the collection of known nodes will have a wide representation of IDs. The neighborhood set is used when populating and repairing the routing table, but is never used during routing.

The neighborhood set is populated by calculating the proximity metric for the inserted node from the current node, then compared to other nodes in the neighborhood set. The neighborhood set is sorted by proximity metric score, from lowest (best) to highest (worst). When a new node is inserted, the 32 nodes with the lowest scores are retained, and any extras are discarded.

The neighborhood set is repaired by requesting the neighborhood set of every other node in the neighborhood set, then calculating the proximity metric for the received nodes. These nodes are all in close proximity to the current node, so the nodes in close proximity to _them_ are likely to contain a suitable replacement.

## Routing

Routing a message through the cluster is a simple process of finding a suitable node in the state tables, then forwarding to that node. Should no suitable node be found, the message has reached its destination and is considered "delivered".

The message ID is the key tool used in routing the message through the cluster. The node with the ID closest to the message ID is the destination for the message, and each routing step should bring the message closer to that destination.

The first state table consulted when routing a message is the leaf set. If the message ID falls within the leaf set, then the current node knows the destination of the message, and forwards the message there.

If the message ID falls outside the leaf set, the routing table is consulted. The shared prefix between the message ID and the current node's ID is calculated, which determines the row that is consulted in the routing table. The column is the value of the first different digit in the message ID. If a node exists at that column in the routing table, the message is forwarded to that node.

If no node exists at that row and column in the routing table, the rest of the row is searched for a node that is closer in proximity to the message ID than the current node. If such a node is found, the message is forwarded to that node.

If no node in the row is closer to the message ID than the current node, lower rows (high indices) are searched for a node closer to the message ID than the current node. If such a node is found, the message is forwarded to that node.

If no such node can be found, the current node is the most appropriate node in the cluster, and should be considered the destination for the message. At this point, the message is considered "delivered".

## Joining the Cluster

When a node wishes to join the cluster, it needs to know the IP and port of another node in the cluster. This node is assumed to be the closest to the joining node in the network topology, though if a sub-optimal node is chosen, only the locality properties of routing will be affected. Essentially, Wendy will be a little slower, but everything should still work.

The joining node crafts a message with a message ID equal to its node ID. This special "join" message is then sent to the specified node, which then routes it like any other message. Each node that receives the message sends their routing table to the joining node. The node the message was originally sent to, because it is assumed to be the closest node in the network topology, sends its neighborhood set to the joining node, as nodes close to it should be close to the joining node. Finally, the destination node for the message also includes sends its leaf set to the joining node.

When the node receives routing table information, it attempts to insert the nodes in the received routing table into its own routing table. For unknown nodes with unknown proximities, it checks its local proximity cache (to reduce the number of repeat requests made). If no number is found, it makes a request to the inserted node to determine its proximity, then caches that proximity score in its proximity cache. Each node is also evaluated for inclusion in the neighborhood set as they're being inserted into the routing table.

When the node receives leaf set information, it uses that leaf set as the basis of its own leaf set. The node that the message is delivered to is the node with the closest node ID to its own, so the nodes closest to that node in the node ID space are the nodes closest to it in the node ID space and appropriate choices for the leaf set.

When the node receives neighborhood set information, it uses that neighborhood set as the basis of its own neighborhood set. The node the neighborhood set comes from _should_ be the closest node in the network topology, and assuming the proximity metric is Euclidean (i.e., if point A is close to point B and point C is close to point A, point C is also close to point B then; this holds true in the current implementation) the nodes closest to that node should be the nodes closest to this node. Wendy makes a cursory effort to correct mistakes here by gauging the appropriateness of every node it encounters for the neighborhood set, but it is still possible to create a sub-optimal neighborhood set in larger clusters. Wendy will still continue to function, though its routing paths will be less optimal than they would be with a proper neighborhood set.