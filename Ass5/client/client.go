package main

import (
	"bufio"
	"context"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
	"time"

	proto "Auction/proto"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

const (
	serverAddr       = "localhost:50051"
	backupServerAddr = "localhost:50052"
)

// Returns true if the RPC error could indicate an unreachable server.
func isConnectionFailure(err error) bool {
	code := status.Code(err)
	return code == codes.DeadlineExceeded ||
		code == codes.Unavailable
}

// Try the leader first, then the backup if communication fails.
func sendBid(
	leader proto.AuctionClient,
	backup proto.AuctionClient,
	req *proto.BidderData,
) (*proto.BidAck, error) {

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	response, err := leader.Bid(ctx, req)
	cancel()

	if err == nil {
		return response, nil
	}

	if !isConnectionFailure(err) {
		return nil, err
	}

	log.Printf("Component=Client Event=LeaderUnreachable Address=%s Error=%v",
		serverAddr, err)
	log.Printf("Component=Client Event=RetryBid Address=%s Amount=%d",
		backupServerAddr, req.Amount)

	retryCtx, retryCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer retryCancel()

	response, err = backup.Bid(retryCtx, req)

	if err != nil {
		log.Printf("Component=Client Event=BackupBidFailed Address=%s Error=%v",
			backupServerAddr, err)
		return nil, err
	}

	return response, nil
}

// Try the leader first, then the backup if communication fails.
func getResult(
	leader proto.AuctionClient,
	backup proto.AuctionClient,
) (*proto.Outcome, error) {

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	response, err := leader.Result(ctx, &proto.Empty{})
	cancel()

	if err == nil {
		return response, nil
	}

	if !isConnectionFailure(err) {
		return nil, err
	}

	log.Printf("Component=Client Event=LeaderUnreachable Address=%s Error=%v",
		serverAddr, err)
	log.Printf("Component=Client Event=RetryResult Address=%s",
		backupServerAddr)

	retryCtx, retryCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer retryCancel()

	response, err = backup.Result(retryCtx, &proto.Empty{})

	if err != nil {
		log.Printf("Component=Client Event=BackupResultFailed Address=%s Error=%v",
			backupServerAddr, err)
		return nil, err
	}

	return response, nil
}

func main() {

	personalMaxBid := int64(0)

	// Create reusable connections to both nodes.
	conn, err := grpc.NewClient(
		serverAddr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		log.Fatalf("Component=Client Event=ConnectionSetupFailed Address=%s Error=%v",
			serverAddr, err)
	}
	defer conn.Close()

	backupConn, err := grpc.NewClient(
		backupServerAddr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		log.Fatalf("Component=Client Event=ConnectionSetupFailed Address=%s Error=%v",
			backupServerAddr, err)
	}
	defer backupConn.Close()

	bidderClient := proto.NewAuctionClient(conn)
	backupClient := proto.NewAuctionClient(backupConn)

	scanner := bufio.NewScanner(os.Stdin)

	fmt.Print("Type a username: ")
	if !scanner.Scan() {
		return
	}

	bidderID := strings.TrimSpace(scanner.Text())

	if bidderID == "" {
		fmt.Println("Username cannot be empty.")
		return
	}

	fmt.Printf("Welcome %s! Commands: bid <amount>, result, exit\n", bidderID)

	for {
		fmt.Print("> ")

		if !scanner.Scan() {
			break
		}

		text := strings.Fields(scanner.Text())

		if len(text) == 0 {
			continue
		}

		switch text[0] {

		case "bid":

			if len(text) != 2 {
				fmt.Println("Usage: bid <amount>")
				continue
			}

			currentBid, err := strconv.ParseInt(text[1], 10, 64)

			if err != nil || currentBid <= 0 {
				fmt.Println("Invalid bid amount. Enter a positive integer.")
				continue
			}

			if currentBid <= personalMaxBid {
				fmt.Printf("Bid must be greater than your previous accepted bid of %d\n",
					personalMaxBid)
				continue
			}

			log.Printf("Component=Client Event=BidSubmitted BidderID=%s Amount=%d",
				bidderID, currentBid)

			bidattempt, err := sendBid(
				bidderClient,
				backupClient,
				&proto.BidderData{
					BidderName: bidderID,
					Amount:     currentBid,
				},
			)

			if err != nil {
				log.Printf("Component=Client Event=BidRequestFailed BidderID=%s Amount=%d Error=%v",
					bidderID, currentBid, err)
				fmt.Println("Could not confirm the bid. Check the auction result before retrying.")
				continue
			}

			switch bidattempt.GetAck() {

			case proto.BidAckEnum_SUCCESS:

				personalMaxBid = currentBid

				log.Printf("Component=Client Event=BidAccepted BidderID=%s Amount=%d",
					bidderID, currentBid)

				fmt.Printf("Bid of %d placed successfully.\n", currentBid)

			case proto.BidAckEnum_FAIL:

				log.Printf("Component=Client Event=BidRejected BidderID=%s Amount=%d",
					bidderID, currentBid)

				fmt.Printf("Bid of %d rejected. Amount is too low or invalid.\n",
					currentBid)

			case proto.BidAckEnum_EXCEPTION:

				log.Printf("Component=Client Event=AuctionEnded BidderID=%s",
					bidderID)

				fmt.Println("Auction has ended. No more bids are accepted.")

			default:
				log.Printf("Component=Client Event=UnknownBidResponse Ack=%v",
					bidattempt.GetAck())
			}

		case "result":

			log.Printf("Component=Client Event=ResultRequested BidderID=%s",
				bidderID)

			currentWinner, err := getResult(bidderClient, backupClient)

			if err != nil {
				log.Printf("Component=Client Event=ResultRequestFailed Error=%v", err)
				fmt.Println("Could not retrieve the auction result.")
				continue
			}

			maxBid := currentWinner.GetHighestBid()
			winnerName := currentWinner.GetBidderName()
			ongoing := currentWinner.GetAuctionOngoing()

			if winnerName == "" {
				if ongoing {
					fmt.Println("No bids have been placed yet.")
				} else {
					fmt.Println("Auction ended with no bids.")
				}
				continue
			}

			if ongoing {
				if winnerName == bidderID {
					fmt.Printf("You are currently winning with a bid of %d.\n",
						maxBid)
				} else {
					fmt.Printf("Current highest bidder: %s, amount: %d.\n",
						winnerName, maxBid)
				}
			} else {
				if winnerName == bidderID {
					fmt.Printf("Auction ended! You won with a bid of %d.\n",
						maxBid)
				} else {
					fmt.Printf("Auction ended. Winner: %s, amount: %d.\n",
						winnerName, maxBid)
				}
			}

		case "exit":
			fmt.Println("Exiting auction client.")
			return

		default:
			fmt.Println("Unknown command. Use: bid <amount>, result, exit")
		}
	}

	if err := scanner.Err(); err != nil {
		log.Printf("Component=Client Event=InputError Error=%v", err)
	}
}
