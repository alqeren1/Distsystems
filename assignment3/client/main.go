package main

import (
	"context" // Used to control the lifetime of the RPC request.
	"fmt"     // Used to print values to the terminal.
	"log"     // Used to print errors and stop the program.
	"time"    // Used for the timeout and to convert Unix nanoseconds back to a readable time.

	// This imports the Go code generated from your .proto file.
	// It gives us:
	// - proto.Empty
	// - proto.TimeResponse
	// - proto.NewTimeKeeperClient
	proto "timeapp/grpc/proto"

	// Main gRPC package.
	// We use this to connect to the gRPC server.
	"google.golang.org/grpc"

	// We are not using TLS/security for this local exercise.
	// So this package lets us create an insecure connection.
	"google.golang.org/grpc/credentials/insecure"
)

func main() {

	// Create a connection to the gRPC server.
	//
	// "localhost:50051" means:
	// - localhost = this same computer
	// - 50051 = the port where our server is listening
	//
	// insecure.NewCredentials() means we are not using TLS encryption.
	// That is fine for a simple local course exercise.
	conn, err := grpc.Dial(
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

	// Create a TimeKeeper client using the connection.
	//
	// NewTimeKeeperClient was automatically generated from:
	//
	// service TimeKeeper {
	//     rpc GetTime(Empty) returns (TimeResponse);
	// }
	//
	// This object gives us methods such as:
	//
	// client.GetTime(...)
	client := proto.NewTimeKeeperClient(conn)

	// Create a context with a 3-second timeout.
	//
	// This means:
	// if the RPC takes longer than 3 seconds,
	// cancel the request instead of waiting forever.
	//
	// context.Background() is the base/default context.
	ctx, cancel := context.WithTimeout(
		context.Background(),
		3*time.Second,
	)

	// Release resources related to the context when main() ends.
	defer cancel()

	// Call the remote GetTime method.
	//
	// This looks like a normal Go function call,
	// but it actually sends a gRPC request over the network.
	//
	// ctx:
	// controls timeout/cancellation.
	//
	// &proto.Empty{}:
	// is the request message.
	//
	// Our .proto says:
	//
	// message Empty {}
	//
	// so there is no data to put inside the request.
	//
	// resp will contain the TimeResponse returned by the server.
	resp, err := client.GetTime(
		ctx,
		&proto.Empty{},
	)

	// If the RPC fails, stop the program.
	//
	// For example:
	// - server is not running
	// - wrong IP
	// - wrong port
	// - timeout
	if err != nil {
		log.Fatal(err)
	}

	// Print the raw value received from the server.
	//
	// Your proto says:
	//
	// message TimeResponse {
	//     int64 unix_nanos = 1;
	// }
	//
	// protobuf converts unix_nanos into the Go field:
	//
	// UnixNanos
	fmt.Println("Unix nanos:", resp.UnixNanos)

	// Convert the Unix nanosecond value back into Go's time.Time.
	//
	// time.Unix expects:
	//
	// time.Unix(seconds, nanoseconds)
	//
	// We currently have the entire timestamp as nanoseconds,
	// so we use:
	//
	// seconds = 0
	// nanoseconds = resp.UnixNanos
	//
	// Go normalizes this correctly.
	readableTime := time.Unix(
		0,
		resp.UnixNanos,
	)

	// Print the human-readable version.
	fmt.Println("Readable time:", readableTime)
}