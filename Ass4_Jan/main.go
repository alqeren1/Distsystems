package main

import (
	"Ass4/proto"
	"bufio"
	"context"
	"flag"
	"log"
	"net"
	"os"

	"sync"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)
var nodePorts = []string{":50051", ":50052", ":50053"}

type NodeMemoryStruct struct {
	mu sync.Mutex
	nodeID  int64
	t           int64 // Current Lamport clock
	requestTime int64 // Timestamp of our pending CS request
	state   string
	clients []proto.Ass4Client
	deferred []chan struct{}
	proto.UnimplementedAss4Server
}

func (nm *NodeMemoryStruct) RequestAccess(ctx context.Context, req *proto.ReqData) (*proto.ReplyAccessGranted, error) {
	log.Print(req.NodeId, req.T)
	return &proto.ReplyAccessGranted{}, nil
}

func runNode(node *NodeMemoryStruct, ownPort string) {
	log.Println("Running Node")

	for _, adr := range nodePorts {
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

		node.clients = append(node.clients, client)

	}
	log.Printf("clients created: %d", len(node.clients))
	scanner := bufio.NewScanner(os.Stdin)
	for scanner.Scan() {
		log.Println("Enter pressed, testing concurrent requests.")

		granted := node.requestPermissions(1)

		log.Printf("All peers granted: %t", granted)
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
