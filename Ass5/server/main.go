package main

import (
	"context"
	"log"
	"net"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	proto "Auction/proto"

	"google.golang.org/grpc"
)

type auctionServer struct {
	proto.UnimplementedAuctionServer
	proto.UnimplementedReplicationNodesServer

	mu             sync.Mutex
	highestBid     int64
	highestBidder  string
	bidders        map[string]int64
	auctionOngoing bool
}

func newAuctionServer() *auctionServer {
	return &auctionServer{
		highestBid:     0,
		highestBidder:  "",
		bidders:        make(map[string]int64),
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

	if s.highestBid < bidAmount {
		s.mu.Lock()
		log.Printf("Bid accepted by user %s at amount %d!", bidderName, bidAmount)
		s.highestBid = bidAmount
		s.highestBidder = bidderName
		s.mu.Unlock()

		return &proto.BidAck{Ack: proto.BidAckEnum_SUCCESS}, nil
	} else {
		log.Printf("Bid is too low! User %s amount %d needs to be higher than %d!", bidderName, bidAmount, s.highestBid)
		return &proto.BidAck{Ack: proto.BidAckEnum_FAIL}, nil

	}
}

func (s *auctionServer) Result(ctx context.Context) (*proto.Outcome, error) {

	return &proto.Outcome{
		HighestBid:     s.highestBid,
		AuctionOngoing: s.auctionOngoing,
		BidderName:     s.highestBidder,
	}, nil

}

func (s *auctionServer) AcceptedBid(ctx context.Context, bid *proto.BidderData) (*proto.Empty, error){
	s.highestBid = bid.Amount,
	s.BidderName = bid.BidderName,
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
