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

	proto "Auction/proto"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type auctionServer struct {
	proto.UnimplementedAuctionServer

	mu           sync.Mutex
	highestBid int64
	highestBidder string
	bidders      map[string]int64
	auctionOngoing bool
}

func newAuctionServer() *auctionServer {
	return &auctionServer{
		highestBid: 0,
		highestBidder: "",
        bidders:    make(map[string]int64),
		auctionOngoing: true,

	}
}

func (s *auctionServer) Bid(
	ctx context.Context,
	req *proto.BidderData,
) (*proto.BidAck, error) {

	bidderName := req.BidderName
	bidAmount := req.Amount

	s.mu.Lock()
	// checking if bidder name is available
	if _, exists := s.bidders[bidderName]; exists {
	s.mu.Unlock()
    log.Print("Bidder already registered, placing the bid")
    } else {
	s.bidders[bidderName] = bidAmount
	s.mu.Unlock()
	}
	
	// Checking if it is an accepted bid

	if s.highestBid<bidAmount{
		s.mu.Lock()
		log.Printf("Bid accepted by user %s at amount %d!", bidderName,bidAmount)
		s.highestBid = bidAmount
		s.highestBidder = bidderName
		s.mu.Unlock()

		return &proto.BidAck{Ack: proto.BidAckEnum_SUCCESS,}, nil
	}else{
		log.Printf("Bid is too low! User %s amount %d needs to be higher than %d!", bidderName,bidAmount,s.highestBid)
		return &proto.BidAck{Ack: proto.BidAckEnum_FAIL,}, nil


	}
}

func (s *auctionServer) Result(ctx context.Context) (*proto.Outcome, error) {

	return &proto.Outcome{
		HighestBid: s.highestBid,
		AuctionOngoing: s.auctionOngoing,
		BidderName: s.highestBidder,
	}, nil

}

func (s *chatServer) removeClient(clientid string) {

	s.mu.Lock()
	clientChannel, exists := s.clients[clientid]

	if exists {
		delete(s.clients, clientid)
		close(clientChannel)
	} else {
		s.mu.Unlock()
		return
	}
	s.logicalTime++
	currentTime := s.logicalTime
	message := fmt.Sprintf(
		"Participant %s left Chit Chat at logical time %d",
		clientid,
		currentTime,
	)
	broadcastMsg := &proto.BroadcastMessage{
		ClientId:    clientid,
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

) (*proto.LeaveAccepted, error) {

	clientID := req.ClientId
	timestamp := req.LogicalTime
	timestamp ++

	s.removeClient(clientID)

	logicalClock := max(timestamp,s.logicalTime) 
	//recieved log
	s.mu.Lock()
	s.logicalTime = logicalClock+1
	s.mu.Unlock()
	//sending leave message from server log

	return &proto.LeaveAccepted{LogicalTime: s.logicalTime}, nil

}

func (s *chatServer) broadcast(msg *proto.BroadcastMessage) {
	slowClients := []string{}
	s.mu.Lock()
	// happens at the end
	defer func() {
		s.mu.Unlock()
		//remove slow clients
		for _, clientID := range slowClients {

			s.removeClient(clientID)

		}
	}()
	//send message to all clients
	for clientID, clientChannel := range s.clients {
		select {
		case clientChannel <- msg:
		// detect slow clients to remove at the end
		case <-time.After(500 * time.Millisecond):
			log.Printf(
				"Component=Server Event=SlowClientTimeout ClientID=%s",
				clientID)
			slowClients = append(slowClients, clientID)

		}

	}
}

func (s *chatServer) Publish(
	ctx context.Context,
	req *proto.PublishReq,
) (*proto.PublishAccepted, error) {

	clientID := req.ClientId
	message := req.Message
	clientclock := req.LogicalTime

	if !utf8.ValidString(message) {
		return nil, status.Error(codes.InvalidArgument, "message must be valid UTF-8")
	}
	if utf8.RuneCountInString(message) > 128 {
		return nil, status.Error(codes.InvalidArgument, "message exceeds 128 characters")
	}

	s.mu.Lock()
	correctTime := max(clientclock,s.logicalTime) + 1
	s.logicalTime = correctTime
	


	log.Printf(
		"Component=Server Event=MessageReqRecieved ClientID=%s LogicalTime=%d Message=%q",
		clientID,
		correctTime,
		message,
	)
	
	s.logicalTime++
	correctTime = s.logicalTime
	
	broadcastMsg := &proto.BroadcastMessage{
		ClientId:    clientID,
		Content:     message,
		LogicalTime: correctTime,
	}
	s.mu.Unlock()
	s.broadcast(broadcastMsg)
	log.Printf(
		"Component=Server Event=MessageBroadcast ClientID=%s LogicalTime=%d Message=%q",
		clientID,
		correctTime,
		message,
	)
	return &proto.PublishAccepted{}, nil
}

func main() {
	listener, err := net.Listen("tcp", ":50051")
	if err != nil {
		log.Fatalf("Component=Server Event=ListenFailed Address=:50051 Error=%q", err)
	}

	grpcServer := grpc.NewServer()
	server := newAuctionServer()
	proto.RegisterAuctionServer(
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
