package main

import (
	proto "Auction/proto"
	"bufio"
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
	"time"

	"google.golang.org/grpc"
)

func main() {
	maxbid := int64(0)
	personalMaxBid := int64(0)
	serverAddr := "localhost:50051"
	backupServerAddr := "localhost:50052"

	conn, err := grpc.NewBidder(serverAddr, grpc.WithInsecure())

	if err != nil {
		log.Fatalf("Component=Client Event=ConnectionSetupFailed Address=%s Error=%q", serverAddr, err)
	}
	bidderClient := proto.NewAuctionClient(conn)

	defer conn.Close()

	fmt.Printf("Type a username: ")
	scanner := bufio.NewScanner(os.Stdin)
	scanner.Scan()
	bidderID := scanner.Text()
	for scanner.Scan() {

		text := strings.Split(scanner.Text(), " ")

		if text[0] == "bid" {

			bid, err := strconv.Atoi(text[1])

			if bid > 0 && err == nil {
				current_bid := int64(bid)
				if current_bid > personalMaxBid {

					personalMaxBid = current_bid

					log.Printf("Component=Client Event=BidPlaced BidderID=%s Amount=%d", bidderID, current_bid)
					ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
					defer cancel()
					bidattempt, err := bidderClient.Bid(
						ctx,
						&proto.BidderData{
							BidderName: bidderID,
							Amount:     int64(current_bid),
						},
					)
					if errors.Is(err, context.DeadlineExceeded) {
						conn.Close()
						conn, err = grpc.Dial(backupServerAddr, grpc.WithInsecure())
						if err != nil {
							log.Fatalf("Component=Client Event=BackupConnectionSetupFailed Address=%s Error=%q", backupServerAddr, err)
						}
						bidderClient = proto.NewAuctionClient(conn)
						retryCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
						defer cancel()
						bidattempt, err = bidderClient.Bid(
							retryCtx,
							&proto.BidderData{
								BidderName: bidderID,
								Amount:     int64(current_bid),
							},
						)
						if errors.Is(err, context.DeadlineExceeded) {
							log.Fatalf("Component=Client Event=BackupServerUnreachable Address=%s Error=%q", backupServerAddr, err)
						}
					}
					switch bidattempt.GetAck() {

					case proto.BidAckEnum_SUCCESS:
						fmt.Printf("Bid of %d placed successfully\n", current_bid)
						maxbid = current_bid

					case proto.BidAckEnum_FAIL:
						fmt.Printf("Bid of %d failed.\n", current_bid)

					case proto.BidAckEnum_EXCEPTION:
						fmt.Printf("Bid of %d failed due to server exception. Please try again later.\n", current_bid)
					}
				} else {
					fmt.Printf("Bid must be greater than the current max bid of %d\n", personalMaxBid)
				}
			} else {
				fmt.Printf("Invalid bid amount. Please enter a positive integer.\n")
			}
		} else if text[0] == "result" {

			log.Printf("Component=Client Event=StatusRequest BidderID=%s MaxBid=%d", bidderID, personalMaxBid)
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			current_winner, err := bidderClient.Result(ctx, &proto.Empty{})
			if errors.Is(err, context.DeadlineExceeded) {
				conn.Close()
				conn, err = grpc.Dial(backupServerAddr, grpc.WithInsecure())
				if err != nil {
					log.Fatalf("Component=Client Event=BackupConnectionSetupFailed Address=%s Error=%q", backupServerAddr, err)
				}
				bidderClient = proto.NewAuctionClient(conn)
				retryCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
				defer cancel()
				current_winner, err = bidderClient.Result(retryCtx, &proto.Empty{})
				if errors.Is(err, context.DeadlineExceeded) {
					log.Fatalf("Component=Client Event=BackupServerUnreachable Address=%s Error=%q", backupServerAddr, err)
				}
			}
			maxbid = current_winner.HighestBid
			current_winner_name := current_winner.BidderName

			if current_winner_name == bidderID {
				fmt.Printf("You are currently winning with a bid of %d\n", maxbid)
			} else {
				fmt.Printf("You are currently losing. The current winning bidder is %s with a bid of %d\n", current_winner_name, maxbid)
			}
		}
	}
}
