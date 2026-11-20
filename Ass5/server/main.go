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
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

type auctionServer struct {
	proto.UnimplementedAuctionServer
	 proto.UnimplementedReplicationNodesServer

	mu           sync.Mutex
	mu2  sync.Mutex
	isLeader bool
	highestBid int64
	highestBidder string
	bidders      map[string]int64
	auctionOngoing bool
	startTime int64
	auctionDuration int64

	peer proto.ReplicationNodesClient 
}

func newAuctionServer(leader bool) *auctionServer {
	return &auctionServer{
		highestBid: 0,
		isLeader: leader,
		highestBidder: "",
        bidders:    make(map[string]int64),
		auctionOngoing: true,
		startTime: time.Now().Unix(),
		auctionDuration: 1000,

	}
}

func (s *auctionServer) Bid(
	ctx context.Context,
	req *proto.BidderData,
) (*proto.BidAck, error) {

	s.mu.Lock()
	
	
	
	bidderName := req.BidderName
	bidAmount := req.Amount
	log.Printf("Component=Server Event=BidReceived BidderID=%s Amount=%d",
    bidderName, bidAmount)

	
	leader := s.isLeader
	s.mu.Unlock()

	skipForward := false // ADDED

	if !leader {
		// Backup forwards the bid to the leader
		forwardCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		log.Printf("Component=Server Event=BidForwardedToLeader BidderID=%s Amount=%d",
    	bidderName, bidAmount)
		response, err := s.peer.ForwardBid(forwardCtx, req)
		cancel()

		if err == nil {
			log.Printf("Component=Server Event=BidForwardResponse Ack=%s",
        	response.GetAck().String())
			return response, nil
		}

		if ctx.Err() != nil {
			return nil, err
		}

		if status.Code(err) == codes.DeadlineExceeded ||
			status.Code(err) == codes.Unavailable {

			

			s.mu.Lock()
			s.isLeader = true
			s.mu.Unlock()
			log.Printf("Component=Server Event=LeaderUnreachable Action=PromoteToLeader Error=%v", err)

			skipForward = true // ADDED: Don't forward again
		} else {
			return nil, err
		}
	}

	// Leader replicates to backup, unless peer is unreachable
	s.mu2.Lock()
	defer s.mu2.Unlock()

	s.mu.Lock()

	if s.startTime + s.auctionDuration <= time.Now().Unix(){
		s.auctionOngoing = false
		log.Printf("Component=Server Event=AuctionEnded HighestBid=%d",
    	s.highestBid)
		s.mu.Unlock()
		
		return &proto.BidAck{Ack: proto.BidAckEnum_EXCEPTION}, nil

	}
	if !s.auctionOngoing {
		s.mu.Unlock()
		return &proto.BidAck{Ack: proto.BidAckEnum_EXCEPTION}, nil
	}

	if bidAmount <= s.highestBid {
		log.Printf("Component=Server Event=BidRejected Reason=TooLow BidderID=%s Amount=%d HighestBid=%d",
    	bidderName, bidAmount, s.highestBid)
		s.mu.Unlock()
		return &proto.BidAck{Ack: proto.BidAckEnum_FAIL}, nil
	}

	s.mu.Unlock()
	if !skipForward { // ADDED
		
		forwardCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		log.Printf("Component=Server Event=ReplicationStarted BidderID=%s Amount=%d",
    	bidderName, bidAmount)
		response, err := s.peer.ForwardBid(forwardCtx, req)
		cancel()

		if err != nil {
			if ctx.Err() != nil {
				return nil, err
			}

			if status.Code(err) == codes.DeadlineExceeded ||
				status.Code(err) == codes.Unavailable {

				log.Printf("Component=Server Event=ReplicationUnavailable Action=ContinueLocally Error=%v", err)

			} else {
				log.Printf("Component=Server Event=ReplicationRPCFailed Error=%v", err)
				return nil, err
			}

		} else {

			if response.GetAck() == proto.BidAckEnum_SUCCESS {
				log.Printf("Component=Server Event=ReplicationAcknowledged BidderID=%s Amount=%d",
    bidderName, bidAmount)

			} else if response.GetAck() == proto.BidAckEnum_FAIL {
				log.Printf("Component=Server Event=ReplicationRejected BidderID=%s Amount=%d",
    bidderName, bidAmount)
				return &proto.BidAck{Ack: proto.BidAckEnum_FAIL}, nil

			} else if response.GetAck() == proto.BidAckEnum_EXCEPTION {
				log.Printf("Component=Server Event=ReplicationException BidderID=%s Amount=%d",
    bidderName, bidAmount)
				return &proto.BidAck{Ack: proto.BidAckEnum_EXCEPTION}, nil
			}
		}
	}

	// Check and accept bid locally
	s.mu.Lock()

	if s.highestBid < bidAmount {
		s.bidders[bidderName] = bidAmount

		log.Printf("Component=Server Event=BidAccepted BidderID=%s Amount=%d",
    bidderName, bidAmount)

		s.highestBid = bidAmount
		s.highestBidder = bidderName
		s.mu.Unlock()
		
		return &proto.BidAck{Ack: proto.BidAckEnum_SUCCESS}, nil

	} else {
		log.Printf("Component=Server Event=BidRejected Reason=TooLow BidderID=%s Amount=%d HighestBid=%d",
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
    
	
	leader := s.isLeader
	log.Printf("Component=Server Event=ForwardBidReceived BidderID=%s Amount=%d IsLeader=%t",
    req.BidderName, req.Amount, leader)
	

	if leader {
		log.Printf("Component=Server Event=ForwardedBidDelegated BidderID=%s Amount=%d",
    	req.BidderName, req.Amount)
		s.mu.Unlock()
		
		return s.Bid(ctx, req)
	}
	
	
	

    if req.Amount <= s.highestBid {
		log.Printf("Component=Server Event=ReplicationRejected Reason=TooLow BidderID=%s Amount=%d HighestBid=%d",
    	req.BidderName, req.Amount, s.highestBid)
		s.mu.Unlock()
        return &proto.BidAck{Ack: proto.BidAckEnum_FAIL}, nil
    }
	s.bidders[req.BidderName] = req.Amount
    s.highestBid = req.Amount
    s.highestBidder = req.BidderName
    
	

    log.Printf("Component=Server Event=BidReplicated BidderID=%s Amount=%d",
    req.BidderName, req.Amount)
	s.mu.Unlock()
    return &proto.BidAck{Ack: proto.BidAckEnum_SUCCESS}, nil
}


func (s *auctionServer) Result(
    ctx context.Context,
    req *proto.Empty,
) (*proto.Outcome, error) {

    s.mu.Lock()
    leader := s.isLeader
    s.mu.Unlock()
	log.Printf("Component=Server Event=ResultRequested IsLeader=%t", leader)

    // CASE 1: Backup receives Result()
    if !leader {
		log.Printf("Component=Server Event=ResultForwardedToLeader")
        forwardCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
        response, err := s.peer.ForwardResult(forwardCtx, req)
        cancel()

        if err == nil {
			log.Printf("Component=Server Event=ResultForwardSucceeded HighestBid=%d",
        response.GetHighestBid())
            return response, nil
        }

        if ctx.Err() != nil {
            return nil, err
        }

        if status.Code(err) == codes.DeadlineExceeded ||
            status.Code(err) == codes.Unavailable {

            

            s.mu.Lock()
            s.isLeader = true
            s.mu.Unlock()
			log.Printf("Component=Server Event=LeaderUnreachable Action=PromoteToLeader Source=Result Error=%v",
    err)

            // Continue below and return local state

        } else {
            return nil, err
        }
    }

    // CASE 2: Leader returns its local auction state
    s.mu.Lock()
    defer s.mu.Unlock()

    if time.Now().Unix() >= s.startTime+s.auctionDuration {
        s.auctionOngoing = false
    }
	log.Printf("Component=Server Event=ResultReturned HighestBid=%d HighestBidder=%s AuctionOngoing=%t",
    s.highestBid, s.highestBidder, s.auctionOngoing)
    return &proto.Outcome{
        HighestBid:     s.highestBid,
        AuctionOngoing: s.auctionOngoing,
        BidderName:     s.highestBidder,
    }, nil
}
func (s *auctionServer) ForwardResult(
    ctx context.Context,
    req *proto.Empty,
) (*proto.Outcome, error) {

    s.mu.Lock()
	log.Printf("Component=Server Event=ForwardResultReceived")
    defer s.mu.Unlock()

    // Update auction status
    if time.Now().Unix() >= s.startTime+s.auctionDuration {
        s.auctionOngoing = false
    }
	log.Printf("Component=Server Event=ForwardResultReturned HighestBid=%d HighestBidder=%s AuctionOngoing=%t",
    s.highestBid, s.highestBidder, s.auctionOngoing)
    return &proto.Outcome{
        HighestBid:     s.highestBid,
        AuctionOngoing: s.auctionOngoing,
        BidderName:     s.highestBidder,
    }, nil
}

func (s *auctionServer) SyncTime(
    ctx context.Context,
    req *proto.Empty,
) (*proto.Time, error)  {
	s.mu.Lock()
	defer s.mu.Unlock()
	
	log.Printf("Component=Server Event=SyncTimeResponded StartTime=%d",
    s.startTime)
	return &proto.Time{ AuctionStartTime: s.startTime,}, nil
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
	if !leaderN {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
    	defer cancel()
		timeResult,err := server.peer.SyncTime(ctx,&proto.Empty{})

		if err != nil {
        log.Fatalf("Component=Server Event=TimeSyncFailed Error=%v", err)
		} else {
			server.mu.Lock()
			server.startTime = timeResult.AuctionStartTime
			server.mu.Unlock()

			log.Printf("Auction start time synchronized: %d", server.startTime)
		}

	}
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
