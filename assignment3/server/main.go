package main

import (
	"context"
	"fmt"
	"log"
	"net"
	"sync"
	"unicode/utf8"

	"chitchat/chitchat/grpc/proto"
	proto "chitchat/grpc/proto"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type chatServer struct {
	proto.UnimplementedChitchatServer

	mu sync.Mutex
	logicalClock int64
	clients map[string] chan *proto.BroadcastMessage
}

func newChatServer() *chatServer {
    return &chatServer{
        clients: make(map[string]chan *proto.BroadcastMessage),
    }
}

func (s *chatServer) Join(
    req *proto.JoinReq,
    stream proto.Chitchat_JoinServer,
) error {

    clientID := req.ClientId

    clientChannel := make(chan *proto.BroadcastMessage, 10)

    s.mu.Lock()
    s.clients[clientID] = clientChannel
    s.logicalClock++
    currentTime := s.logicalClock
    s.mu.Unlock()

    log.Printf(
        "Component=Server Event=ClientJoin ClientID=%s LogicalTime=%d",
        clientID,
        currentTime,
    )
	//cleanup. It only runs after the whole join function finishes, after for loop ends
	//needed in the case that client didnt call leave and crashed or just closed the app
	defer func() {
		s.mu.Lock()

		if _, exists := s.clients[clientID]; exists {
        delete(s.clients, clientID)
    	}

		s.mu.Unlock()

		log.Printf(
			"Component=Server Event=ClientDisconnected ClientID=%s",
			clientID,
		)
	}()
	//for keeping the connection open, infinate loop
    for {

	//after a clients leaves, and channel is closed, this part makes sure the loop is not running
	//for nothing
    msg, ok := <-clientChannel

	if !ok {
		return nil
	}

    if err := stream.Send(msg); err != nil {
        return err
    }
}
}
func (s *chatServer) removeClient (clientid string)  {
	s.mu.Lock()
    clientChannel, exists := s.clients[clientid]

	if exists {
		delete(s.clients, clientid)
		close(clientChannel)
	} else { 
		s.mu.Unlock()
		return
	}
    s.logicalClock++
    currentTime := s.logicalClock
	message := fmt.Sprintf(
    "Participant %s left Chit Chat at logical time %d",
    clientid,
    currentTime,
)
	broadcastMsg := &proto.BroadcastMessage{
		ClientId: clientid,
		Content:     message,
		LogicalTime: currentTime,
}
	s.mu.Unlock()

	s.broadcast(broadcastMsg)
	log.Printf(
        "Component=Server Event=ClientLeave ClientID=%s LogicalTime=%d",
        clientid,
        currentTime,
    )
    

}

func (s *chatServer) Leave(
	ctx context.Context,
    req *proto.LeaveReq,
    
) (*proto.Empty, error) {

    clientID := req.ClientId

    

    s.mu.Lock()
    clientChannel, exists := s.clients[clientID]

	if exists {
		delete(s.clients, clientID)
		close(clientChannel)
	}
    s.logicalClock++
    currentTime := s.logicalClock
    s.mu.Unlock()

    
	message := fmt.Sprintf(
    "Participant %s left Chit Chat at logical time %d",
    clientID,
    currentTime,
)
	broadcastMsg := &proto.BroadcastMessage{
		ClientId: clientID,
		Content:     message,
		LogicalTime: currentTime,
}
	

	s.broadcast(broadcastMsg)
	log.Printf(
        "Component=Server Event=ClientLeave ClientID=%s LogicalTime=%d",
        clientID,
        currentTime,
    )
	
	return &proto.Empty{}, nil

}

func (s *chatServer) broadcast(msg *proto.BroadcastMessage) {
    s.mu.Lock()
	// happens at the end
    defer s.mu.Unlock()
	//send message to all clients
    for _, clientChannel := range s.clients {
        clientChannel <- msg
    }
}

func (s *chatServer) Publish(
    ctx context.Context,
    req *proto.PublishReq,
) (*proto.Empty, error) {

	clientID := req.ClientId
	message := req.Message

	if !utf8.ValidString(message) {
		return nil, status.Error(codes.InvalidArgument, "message must be valid UTF-8")
	}
	if utf8.RuneCountInString(message) > 128 {
		return nil, status.Error(codes.InvalidArgument, "message exceeds 128 characters")
	}

	s.mu.Lock()
	s.logicalClock++
	currentTime := s.logicalClock
	s.mu.Unlock()

	broadcastMsg := &proto.BroadcastMessage{
		ClientId: clientID,
		Content:     message,
		LogicalTime: currentTime,
}

	s.broadcast(broadcastMsg)
	log.Printf(
    "Component=Server Event=MessageBroadcast ClientID=%s LogicalTime=%d Message=%q",
    clientID,
    currentTime,
    message,
)
	return &proto.Empty{}, nil
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