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

	proto "Auction/proto"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

type auctionServer struct {
	proto.UnimplementedAuctionServer
	 proto.UnimplementedReplicationNodesServer

	mu           sync.Mutex
	isLeader bool
	highestBid int64
	highestBidder string
	bidders      map[string]int64
	auctionOngoing bool

	peer proto.ReplicationNodesClient 
}

func newAuctionServer(leader bool) *auctionServer {
	return &auctionServer{
		highestBid: 0,
		isLeader: leader,
		highestBidder: "",
        bidders:    make(map[string]int64),
		auctionOngoing: true,

	}
}

func (s *auctionServer) Bid(
	ctx context.Context,
	req *proto.BidderData,
) (*proto.BidAck, error) {

	s.mu.Lock()
	if !s.auctionOngoing {
		s.mu.Unlock()
		fmt.Println("Auction is not ongoing anymore!")
		return &proto.BidAck{Ack: proto.BidAckEnum_EXCEPTION}, nil
	}
	s.mu.Unlock()

	bidderName := req.BidderName
	bidAmount := req.Amount

	s.mu.Lock()
	leader := s.isLeader
	s.mu.Unlock()

	skipForward := false // ADDED

	if !leader {
		// Backup forwards the bid to the leader
		forwardCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		response, err := s.peer.ForwardBid(forwardCtx, req)
		cancel()

		if err == nil {
			return response, nil
		}

		if ctx.Err() != nil {
			return nil, err
		}

		if status.Code(err) == codes.DeadlineExceeded ||
			status.Code(err) == codes.Unavailable {

			log.Println("Leader unreachable! Taking over.")

			s.mu.Lock()
			s.isLeader = true
			s.mu.Unlock()

			skipForward = true // ADDED: Don't forward again
		} else {
			return nil, err
		}
	}

	// Leader replicates to backup, unless peer is unreachable
	if !skipForward { // ADDED

		forwardCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		response, err := s.peer.ForwardBid(forwardCtx, req)
		cancel()

		if err != nil {
			if ctx.Err() != nil {
				return nil, err
			}

			if status.Code(err) == codes.DeadlineExceeded ||
				status.Code(err) == codes.Unavailable {

				log.Println("Backup unreachable! Continuing with bid!")

			} else {
				log.Printf("Other gRPC error: %v", err)
				return nil, err
			}

		} else {

			if response.GetAck() == proto.BidAckEnum_SUCCESS {
				fmt.Println("Bid accepted on backup node")

			} else if response.GetAck() == proto.BidAckEnum_FAIL {
				fmt.Println("Bid rejected on backup node")
				return &proto.BidAck{Ack: proto.BidAckEnum_FAIL}, nil

			} else if response.GetAck() == proto.BidAckEnum_EXCEPTION {
				fmt.Println("Auction ended on backup node")
				return &proto.BidAck{Ack: proto.BidAckEnum_EXCEPTION}, nil
			}
		}
	}

	// Check and accept bid locally
	s.mu.Lock()

	if s.highestBid < bidAmount {
		s.bidders[bidderName] = bidAmount

		log.Printf("Bid accepted by user %s at amount %d!",
			bidderName, bidAmount)

		s.highestBid = bidAmount
		s.highestBidder = bidderName
		s.mu.Unlock()

		return &proto.BidAck{Ack: proto.BidAckEnum_SUCCESS}, nil

	} else {
		log.Printf("Bid is too low! User %s amount %d needs to be higher than %d!",
			bidderName, bidAmount, s.highestBid)

		s.mu.Unlock()

		return &proto.BidAck{Ack: proto.BidAckEnum_FAIL}, nil
	}
}
func (s *auctionServer) ForwardBid(
    ctx context.Context,
    req *proto.BidderData,
) (*proto.BidAck, error) {

   
	s.mu.Lock()
    if !s.auctionOngoing {
		s.mu.Unlock()
        return &proto.BidAck{Ack: proto.BidAckEnum_EXCEPTION}, nil
    }

	
	leader := s.isLeader
	

	if leader {
		s.mu.Unlock()
		return s.Bid(ctx, req)
	}
	
	
	

    if req.Amount <= s.highestBid {
		s.mu.Unlock()
        return &proto.BidAck{Ack: proto.BidAckEnum_FAIL}, nil
    }
	s.bidders[req.BidderName] = req.Amount
    s.highestBid = req.Amount
    s.highestBidder = req.BidderName
    
	

    log.Printf("Replicated bid from %s: %d", req.BidderName, req.Amount)
	s.mu.Unlock()
    return &proto.BidAck{Ack: proto.BidAckEnum_SUCCESS}, nil
}


func (s *auctionServer) Result(ctx context.Context,
    req *proto.Empty,) (*proto.Outcome, error) {

	s.mu.Lock()
    defer s.mu.Unlock()

	return &proto.Outcome{
		HighestBid: s.highestBid,
		AuctionOngoing: s.auctionOngoing,
		BidderName: s.highestBidder,
	}, nil

}

func (s *auctionServer) AcceptedBid(ctx context.Context, bid *proto.BidderData) (*proto.Empty, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.highestBid = bid.Amount
	s.highestBidder = bid.BidderName

	return &proto.Empty{}, nil
}

func main() {
	listener, err := net.Listen("tcp", ":50051")
	leaderN := false
	if err != nil {
		log.Print("Starting the backup node")
		
		listener2, err2 := net.Listen("tcp", ":50052")
		listener = listener2
		//backup node
		
		if err2 != nil {
		log.Fatalf("Component=Server Event=ListenFailed Address=:50052 Error=%q", err2)}
	}else{
		//leader node
		leaderN = true
		
	}

	grpcServer := grpc.NewServer()
	server := newAuctionServer(leaderN)
	// Determine the other node's address
	peerAddress := "localhost:50051"

	if leaderN {
		peerAddress = "localhost:50052"
	}

	// Create one reusable gRPC connection
	conn, err := grpc.NewClient(
		peerAddress,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		log.Fatalf("Failed to create peer client: %v", err)
	}
	defer conn.Close()

	// Save the replication client in the server
	server.peer = proto.NewReplicationNodesClient(conn)

	proto.RegisterAuctionServer(
		grpcServer,
		server,
	)
	proto.RegisterReplicationNodesServer(grpcServer, server)

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
	
	if leaderN{
		log.Println("Component=Server Event=Startup Port=50051")
	}else{
		log.Println("Component=Server Event=Startup Port=50052")
	}
	

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
