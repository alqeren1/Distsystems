package main

import (
	"context"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"
	"unicode/utf8"

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

    clientChannel := make(chan *proto.BroadcastMessage, 100)

    s.mu.Lock()
	// checking if client name is available
	if _, exists := s.clients[clientID]; exists {
    s.mu.Unlock()
    return status.Error(
        codes.AlreadyExists,
        "client ID is already in use",
    )
}
    s.clients[clientID] = clientChannel
    s.logicalClock++
    currentTime := s.logicalClock
    s.mu.Unlock()
	message := fmt.Sprintf(
    "Participant %s joined Chit Chat at logical time %d",
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
        "Component=Server Event=ClientJoin ClientID=%s LogicalTime=%d",
        clientID,
        currentTime,
    )
	//cleanup. It only runs after the whole join function finishes, after for loop ends
	//needed in the case that client didnt call leave and crashed or just closed the app
	defer func() {
		s.removeClient(clientID)

		log.Printf(
			"Component=Server Event=ClientDisconnected ClientID=%s",
			clientID,
		)
	}()
	//for keeping the connection open, infinate loop
    
	for {
    select {
		//If a broadcast message arrives, handle it.
		case msg, ok := <-clientChannel:
			if !ok {
				return nil
			}

			if err := stream.Send(msg); err != nil {
				return err
			}
			log.Printf(
			"Component=Server Event=MessageSent ClientID=%s LogicalTime=%d Message=%q",
			clientID,
			msg.LogicalTime,
			msg.Content,
)
		// If the gRPC client disconnects, cancels the call, crashes, or the connection dies, 
		// the stream context becomes done.
		case <-stream.Context().Done():
			return nil
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

    

    s.removeClient(clientID)

	
	return &proto.Empty{}, nil

}

func (s *chatServer) broadcast(msg *proto.BroadcastMessage) {
	slowClients := []string{}
    s.mu.Lock()
	// happens at the end
    defer func() {
		s.mu.Unlock()
		//remove slow clients 
		for _, clientID := range slowClients{

		s.removeClient(clientID)

		} }()
	//send message to all clients
    for clientID, clientChannel := range s.clients {
		select{
		case clientChannel <- msg:
		// detect slow clients to remove at the end
		case <- time.After(500 * time.Millisecond):
			log.Printf(
			"Component=Server Event=SlowClientTimeout ClientID=%s",
			clientID,)
			slowClients = append(slowClients,clientID)

		}
        
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
		log.Fatalf("Component=Server Event=ListenFailed Address=:50051 Error=%q", err)
	}

	grpcServer := grpc.NewServer()
	server := newChatServer()
	proto.RegisterChitchatServer(
		grpcServer,
		server,
	)

	// Catch Ctrl+C and termination signals so main can finish normally
	shutdown := make(chan os.Signal, 1)
	signal.Notify(shutdown, os.Interrupt, syscall.SIGTERM)
	defer signal.Stop(shutdown)

	// Serve can return before shutdown finishes, use this channel to wait.
	stopped := make(chan struct{})
	go func() {
		<-shutdown
		log.Println("Component=Server Event=ShutdownStarted")

		// Let ongoing RPCs finish, but force a stop after two seconds.
		// Join streams can otherwise keep GracefulStop waiting indefinitely.
		timer := time.AfterFunc(2*time.Second, grpcServer.Stop)
		grpcServer.GracefulStop()
		timer.Stop()

		close(stopped)
	}()

	log.Println("Component=Server Event=Startup Port=50051")

	err = grpcServer.Serve(listener)
	if err != nil {
		// Unlike log.Fatal, returning allows deferred functions to run.
		log.Printf("Component=Server Event=ServeError Error=%q", err)
		return
	}

	// Log completion only after the shutdown goroutine has finished.
	<-stopped
	log.Println("Component=Server Event=Shutdown")
}
