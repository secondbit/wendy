/* Package pastry implements a fault-tolerant, concurrency-safe distributed hash table.

Self-Organising Services

Pastry is a package to help make your Go programs self-organising. It makes communicating between a variable number of machines easy and reliable. Machines are referred to as Nodes, which create a Cluster together. Messages can then be routed throughout the Cluster.

Getting Started

Getting your own Cluster running is easy. Just create a Node, build a Cluster around it, and announce your presence.

	hostname, err := os.Hostname()
	if err != nil {
		panic(err.Error())
	}
	id, err := pastry.NodeIDFromBytes([]byte(hostname+" test server"))
	if err != nil {
		panic(err.Error())
	}
	node := pastry.NewNode(id, "your_local_ip_address", "your_global_ip_address", "your_region", 8080)

	credentials := pastry.Passphrase("I <3 Gophers.")
	cluster := pastry.NewCluster(node, credentials)
	go func() {
		defer cluster.Stop()
		err := cluster.Listen()
		if err != nil {
			panic(err.Error())
		}
	}()
	cluster.Join("ip of another Node", 8080) // ports can be different for each Node
	select {}

About Credentials

Credentials are an interface that is used to control access to your Cluster. Pastry provides the Passphrase implementation, which limits access to Nodes that set their Credentials to the same string. You can feel free to make your own--the only requirement is that you supply a slice of bytes or an error when the Marshal() function is called and return a boolean when the Valid([]byte) function is called, which should return true if the supplied slice of bytes can be unmarshaled to a valid instance of your Credentials implementation AND that valid instance should be granted access to this Cluster.
*/
package pastry
