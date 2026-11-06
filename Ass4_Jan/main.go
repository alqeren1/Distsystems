package main

import (
	"Ass4/proto"
	"bufio"
	"context"
	"flag"
	"log"
	"net"
	"os"
	"time"

	"sync"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)
var nodePorts = []string{":50051", ":50052", ":50053"}
type deferredRequest struct {
    nodeID    int64
    timestamp int64
    ready     chan struct{}
}
type peerClient struct {
    id     int64
    client proto.Ass4Client
}
type NodeMemoryStruct struct {
	mu sync.Mutex
	nodeID  int64
	t           int64 // Current Lamport clock
	requestTime int64 // Timestamp of our pending CS request
	state   string
	clients []peerClient
	waitQue []deferredRequest
	proto.UnimplementedAss4Server
}

func (nm *NodeMemoryStruct) RequestAccess(ctx context.Context, req *proto.ReqData) (*proto.ReplyAccessGranted, error) {
	log.Print(req.NodeId, req.T)
	
	return nm.checkAccess(ctx, req.NodeId, req.T)
}
func (node *NodeMemoryStruct) enterCriticalSection()  {
	node.mu.Lock()
	 
	node.state = "held"
	node.mu.Unlock()


 	log.Printf("Node %d is accessing the critical section", node.nodeID)
  	time.Sleep(10 * time.Second)
	


}
func (node *NodeMemoryStruct) exitCriticalSection() {

	node.mu.Lock()
	
	node.state = "released"
	log.Printf("Critical section is released by node %d",node.nodeID)
	waiting := node.waitQue
	node.waitQue = nil
	node.mu.Unlock()
	for _, req := range waiting {
        log.Printf(
            "Node %d granting deferred permission to Node %d",
            node.nodeID,
            req.nodeID,
        )
        close(req.ready)
    }
}




func (node *NodeMemoryStruct) resetAfterFailedPermission() {

	node.mu.Lock()
	defer node.mu.Unlock()
	node.state = "released"
	
	waiting := node.waitQue
	node.waitQue = nil
	for _, req := range waiting {
    close(req.ready)
}


}
func (node *NodeMemoryStruct) requestCriticalSection() {

    // MILESTONE 1: Begin requesting
    timestamp, ok := node.beginRequest()

    if !ok {
        log.Println("Node is already requesting or holding CS")
        return
    }

    // MILESTONE 2: Request permission from all peers
    granted := node.requestPermissions(timestamp)

    if !granted {
        log.Println("Not all peers granted permission")
		node.resetAfterFailedPermission()
        return
    }

    // Enter critical section
    node.enterCriticalSection()
	node.exitCriticalSection()




    

    // MILESTONE 4: Release Critical Section
    // TODO: Implement release and deferred replies
}
func runNode(node *NodeMemoryStruct, ownPort string) {
	log.Println("Running Node")

	for i, adr := range nodePorts {
		if adr == ownPort {
			continue
		}
		conn, err := grpc.NewClient(
			"localhost"+adr,
			grpc.WithTransportCredentials(insecure.NewCredentials()),
		)
		if err != nil {
			log.Fatalf("Component=Client Event=ConnectionSetupFailed Address=localhost:50051 Error=%q", err)
		}
		client := proto.NewAss4Client(conn)

		node.clients = append(node.clients, peerClient{
		id:     int64(i + 1),
		client: client,
	})
	
	}
	log.Printf("clients created: %d", len(node.clients))
	scanner := bufio.NewScanner(os.Stdin)
	for scanner.Scan() {
		log.Println("Enter pressed, sending access request")

		node.requestCriticalSection()

		
	}
}
func main() {
	nodeID := flag.Int64("nodeID", -1, "the ID of the node (required)")
	port := flag.String("port", "", "the port for communication (required)")

	flag.Parse()
	if *nodeID == -1 || *port == "" {
		log.Fatal("you must give -nodeID and -port")
	}

	node := &NodeMemoryStruct{nodeID: *nodeID, t: 1, state: "released"}
	log.Print(node.state)

	listener, err := net.Listen("tcp", *port)
	if err != nil {
		log.Fatalf("Component=Server Event=ListenFailed Address=%s Error=%q", *port, err)
	}

	go runNode(node, *port)

	grpcServer := grpc.NewServer()
	proto.RegisterAss4Server(grpcServer, node)

	err = grpcServer.Serve(listener)
	if err != nil {
		// Unlike log.Fatal, returning allows deferred functions to run.
		log.Printf("Component=Server Event=ServeError Error=%q", err)
		return
	}

}
