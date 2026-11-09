README

This is a Ricart-Agrawala distributed mutual exclusion in Go with gRPC.

In the project folder, start three nodes in separate terminals with:  
go run . \-nodeID 1 \-port :50051  
go run . \-nodeID 2 \-port :50052  
go run . \-nodeID 3 \-port :50053  
Optionally, add \-race after run to enable Go’s race detector, which we used when testing.

Node one uses :50051, node two :50052 and node 3 :50053

After having started all three nodes, make a request to access the Critical Section by pressing ENTER in the three terminals.

To stop the terminals press Ctrl \+ c in the terminals.