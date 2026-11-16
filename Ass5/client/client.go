package main

import (
	"bufio"
	"context"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"

	"google.golang.org/grpc"
)

func main() {
	maxbid := 0
	personalMaxBid := 0
	is_winning := false
	serverAddr := "localhost:50051"
	backupServerAddr := "localhost:50052"

	conn, err := grpc.NewBidder(serverAddr, grpc.WithInsecure())

	if err != nil {
		log.Fatalf("Component=Client Event=ConnectionSetupFailed Address=%s Error=%q", serverAddr, err)
	}
	bidderClient := proto.NewBidderClient(conn)

	defer conn.Close()

	fmt.Printf("Type a username: ")
	scanner := bufio.NewScanner(os.Stdin)
	bidderID := scanner.Text()

	for scanner.Scan() {
		text := strings.Split(scanner.Text(), " ")
		if text[0] == "bid" {
			current_bid, err := strconv.Atoi(text[1])
			if current_bid > 0 && err == nil {
				if current_bid > personalMaxBid {
					personalMaxBid = current_bid
					log.Printf("Component=Client Event=BidPlaced BidderID=%s Amount=%d", bidderID, current_bid)
					bid := bidderClient.bid(
						context.Background(),
						&proto.BidderData{
							bidder_Id: bidderID,
							amount:    int32(current_bid),
						},
					)

				} else {
					fmt.Printf("Bid must be greater than the current max bid of %d\n", personalMaxBid)
				}
			} else {
				fmt.Printf("Invalid bid amount. Please enter a positive integer.\n")
			}
		} else if text[0] == "status" {
			log.Printf("Component=Client Event=StatusRequest BidderID=%s MaxBid=%d", bidderID, personalMaxBid)
			current_winner := bidderClient.Result(context.Background())
			maxbid = current_winner.highest_bid
			if current_winner == bidderID {
				is_winning = true
				fmt.Printf("You are currently winning with a bid of %d\n", maxbid)
			} else {
				is_winning = false
				fmt.Printf("You are currently losing. The current winning bidder is %s with a bid of %d\n", current_winner, maxbid)
			}
		}
	}
}
