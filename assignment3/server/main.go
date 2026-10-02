package main

import (
	"context"
	"log"
	"net"
	"time"

	proto "timeapp/grpc/proto"

	"google.golang.org/grpc"
)

type timeServer struct {
	proto.UnimplementedTimeKeeperServer
}

func (s *timeServer) GetTime(
	ctx context.Context,
	req *proto.Empty,
) (*proto.TimeResponse, error) {

	now := time.Now()

	return &proto.TimeResponse{
		UnixNanos: now.UnixNano(),
	}, nil
}

func main() {
	listener, err := net.Listen("tcp", ":50051")
	if err != nil {
		log.Fatal(err)
	}

	grpcServer := grpc.NewServer()

	proto.RegisterTimeKeeperServer(
		grpcServer,
		&timeServer{},
	)

	log.Println("gRPC server running on port 50051")

	err = grpcServer.Serve(listener)
	if err != nil {
		log.Fatal(err)
	}
}