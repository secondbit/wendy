# Pastry

An open source, pure-[Go](http://www.golang.org "Pretty much the best programming language ever") implementation of the [Pastry Distributed Hash Table](http://en.wikipedia.org/wiki/Pastry_(DHT\) "Pastry on Wikipedia").

## Requirements

This implementation of Pastry is written to be compatible with Go 1. It uses nothing outside of the Go standard library. Nodes in the network must be able to communicate using TCP over a configurable port. Nodes also must be able to have long-running processes.

Pastry was developed on OS X 10.8.1, using Go 1.0.3. It has been verified to work as expected running under Ubuntu 12.04 LTS (64-bit), using Go 1.0.3.

## Installation

The typical `go get secondbit.org/pastry` will install Pastry.

## Documentation

We took pains to try and follow [the guidelines](http://golang.org/doc/articles/godoc_documenting_go_code.html "Godoc guidelines on golang.org") on writing good documentation for `godoc`. You can view the generated documentation on the excellent [GoPkgDoc](http://go.pkgdoc.org/secondbit.org/pastry "Pastry's documentation on GoPkgDoc").

## Use

### Initialising the Cluster

The "Cluster" represents your network of nodes. The first thing you should do in any application that uses Pastry is initialise the cluster.

First, you need to create the local Node&mdash;because Pastry is a peer-to-peer algorithm, there's no such thing as a server or client; instead, everything is a "Node", and only Nodes can connect to the Cluster.

```go
hostname, err := os.Hostname()
if err != nil {
	panic(err.Error())
}
id, err := pastry.NodeIDFromBytes([]byte(hostname+" test server"))
if err != nil {
	panic(err.Error())
}
node := pastry.NewNode(id, "your_local_ip_address", "your_global_ip_address", "your_region", 8080)
```

NewNode expects five parameters:

1. The ID of the new Node. We created one in the code sample above. The ID can be any unique string&mdash;it is used to identify the Node to the network. The ID string has to be over 16 bytes long to be substantial enough to form an ID out of, or NodeIDFromBytes will return an error.
2. Your local IP address. This IP address only needs to be accessible to your Region (a concept that will be explained below).
3. Your global IP address. This IP address should be accessible to any Node in your network&mdash;the entire Internet should be able to reach the IP.
4. Your Region. Your Region is a string that helps segment your Pastry network to keep bandwidth minimal. For cloud providers (e.g., EC2), network traffic within a region is free. To take advantage of this, we modified the Pastry algorithm to use the local IP address when two Nodes are in the same Region, and the global IP address the rest of the time, while heavily favouring Nodes that are in the same Region. This allows you to have Nodes in multiple Regions in the same Cluster while minimising your bandwidth costs.
5. The port this Node should listen on, as an int. Should be an open port you have permission to listen on.

Once you have a Node, you can join the Cluster.

```go
cluster := pastry.NewCluster(node)
```

NewCluster just creates a Cluster object, initialises the state tables and channels used to keep the algorithm concurrency-safe, and returns it.

### Listening For Messages

To participate in the Cluster, you need to listen for messages. You'll either be used to pass messages along to the correct Node, or will receive messages intended for your Node.

```go
cluster.Listen()
defer cluster.Stop()
```

`Listen()` is a blocking call, so if you need it to be asynchronous, throw it in a goroutine. **Note**: If you listen twice on the same Cluster in two different goroutines, concurrency-safety **is compromised**. You should only ever have one goroutine Listen to any given Cluster.

`Stop()` ends the Listen call on a Cluster. You'll not receive messages, and will stop participating in the Cluster. It is the graceful way for a Node to exit the Cluster.

### Registering Handlers For Your Application

Pastry offers several callbacks at various points in the process of exchanging messages within your Cluster. You can use these callbacks to register listeners within your application.

**This part has not been implemented yet. It will be documented following implementation.**

## Contributing

We'd love contributions to Pastry. We use it as a mission-critical component in our own applications (**Note:** as Pastry is incomplete, so are our applications) and have a vested interest in seeing it improve.

To contribute to Pastry:

* **Fork** the repository
* **Modify** your fork
* **Send** a pull request
	* Bonus points if the pull request includes *what* you changed, *why* you changed it, and *has unit tests* attached.

We'll review it and merge it in if it's appropriate.

## Implementation Details

We approached this pragmatically, so there are some differences between the Pastry specification (as we understand it) and our implementation. The end result should not be materially changed.

* We *removed the concept of a Neighborhood Set*. The Neighborhood Set was intended to keep information on the proximity of Nodes. We opted to instead store the proximity information inside each Node object.
* We *store every Node in the Routing Table*. The specification dictates that when two Nodes compete for the same space in the Routing Table, only the closest (based on proximity) is stored. We opted to store both, then *route* based on proximity (when routing a message, we select the Node with the closest proximity score). In clusters of sufficiently large size (thousands), this may create memory concerns, as we estimate that Nodes may occupy roughly a couple KB in memory. For our purposes, the memory cost isn't a concern, and it greatly simplifies the algorithm.
* We introduced the concept of Regions. Regions are used to partition your Cluster and give preference to Nodes that are within the same Region. It is useful on cloud providers like EC2 to minimise traffic between regions, which tends to cost more than traffic on the local network. This is implemented as a raw multiplier on the proximity score of nodes, based on if the regions match or not. It should not materially affect the algorithm, outside the intended bias towards local traffic over global traffic.

## Known Bugs

* If you should happen to have two Nodes in a Cluster who don't agree as to what time it is, it's possible to get them stuck in an infinite loop that saturates the network with messages. For the love of God, use NTP to make your Nodes agree what time it is. (*Note*: This is to prevent race conditions when two Nodes join simultaneously.)
* In the event that: 1) a Node is added, 2) the Node receives a message *before* it has finished initialising its state tables, and 3) the Node, based on its partial implementation of the state tables, is the closest Node to the message ID, that Node will incorrectly assume it is the destination for the message when there *may* be a better suited Node in the network. Depending on network speeds and the size of the cluster, this period of potential-for-message-swallowing is expected to last, at most, a few seconds, and will only occur when a Node is added to the cluster. If, as per the previous bug, your Nodes don't agree on the time… well, God help you.

## Authors

The following people contributed code that found its way into Pastry:

* Paddy Foran ([paddy@secondbit.org](mailto:paddy@secondbit.org))

## Contributors

The following people contributed to the creation of Pastry through advice and support, not through code:

* [Matthew Turland](http://www.matthewturland.com) offered support and advice, and has been invaluable in bringing the software to fruition.
* [Chris Hartjes](http://www.littlehart.net/atthekeyboard) offered feedback and advice on our testing strategies.
* [Jesse McNelis](http://jessta.id.au) provided his services both as a bug-hunter and as a rubber duck.
* [Dr. Steven Ko](http://www.cse.buffalo.edu/people/?u=stevko) of the University at Buffalo offered valuable feedback on Pastry and Distributed Hash Tables in general.
* [Jan Newmarch's excellent guide to writing networking code in Go](http://jan.newmarch.name/go/) gave us valuable information.
* [The Go Community](https://groups.google.com/group/go-nuts) (which is superb), offered advice and feedback throughout the creation of this software.

## License

Copyright (c) 2012 Second Bit LLC

Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the "Software"), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
