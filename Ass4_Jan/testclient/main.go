package main

import (
	"Ass4/proto"
	"context"
	"log"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	conn, err := grpc.NewClient(
		"localhost:50051",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)

	// If creating the connection fails, stop the program.
	if err != nil {
		log.Fatalf("Component=Client Event=ConnectionSetupFailed Address=localhost:50051 Error=%q", err)
	}

	// defer means:
	// "run this when main() finishes"
	//
	// So when the client exits, it closes the gRPC connection.
	defer conn.Close()

	client := proto.NewAss4Client(conn)

	_, err = client.RequestAccess(
		context.Background(),
		&proto.ReqData{
			NodeId: 10,
			T:      1,
		},
	)
	id := 100
	if err != nil {
		log.Printf("Component=Client Event=PublishFailed NodeId=%d Error=%q", id, err)
	}
	log.Printf("RequestAccess sent from NodeId=%d", id)
}
