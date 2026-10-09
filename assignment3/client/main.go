package main

import (
	"bufio"
	"context" // Used to control the lifetime of the RPC request.
	"fmt"
	"os"

	// Used to print values to the terminal.
	"log" // Used to print errors and stop the program.

	// Used for the timeout and to convert Unix nanoseconds back to a readable time.
	// This imports the Go code generated from your .proto file.

	proto "chitchat/grpc/proto"

	// We use this to connect to the gRPC server.
	"google.golang.org/grpc"

	// We are not using TLS/security for this local exercise.
	// So this package lets us create an insecure connection.
	"google.golang.org/grpc/credentials/insecure"
)

func main() {

	// Create a connection to the gRPC server.

	conn, err := grpc.NewClient(
		"localhost:50051",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)

	// If creating the connection fails, stop the program.
	if err != nil {
		log.Fatal(err)
	}

	// defer means:
	// "run this when main() finishes"
	//
	// So when the client exits, it closes the gRPC connection.
	defer conn.Close()

	client := proto.NewChitchatClient(conn)

	
	ctx := context.Background()

	fmt.Printf("Type a username: ")
	scanner2 := bufio.NewScanner(os.Stdin)
	scanner2.Scan()
	clientID := scanner2.Text()
	joinReq := &proto.JoinReq{
    ClientId: clientID,
	}
	stream, err := client.Join(ctx, joinReq)
	go func() {
    for {
        msg, err := stream.Recv()

        if err != nil {
            log.Println("stream ended:", err)
            return
        }

        fmt.Printf(
            "[%d][%s] %s\n",
            msg.LogicalTime,
			msg.ClientId,
            msg.Content,
        )

        log.Printf(
            "Component=Client Event=MessageReceived ClientID=%s LogicalTime=%d Message=%q",
            clientID,
            msg.LogicalTime,
            msg.Content,
        )
    }
}()

	scanner := bufio.NewScanner(os.Stdin)

	for scanner.Scan() {
		text := scanner.Text()

		if text == "/leave" {
			_, err := client.Leave(
				context.Background(),
				&proto.LeaveReq{
					ClientId: clientID,
				},
			)

			if err != nil {
				log.Println("leave failed:", err)
			}

			return
		}

		_, err := client.Publish(
			context.Background(),
			&proto.PublishReq{
				ClientId: clientID,
				Message:  text,
			},
		)

    if err != nil {
        log.Println("publish failed:", err)
    }
}
	

	// If the RPC fails, stop the program.
	
	if err != nil {
		log.Fatal(err)
	}

	
}