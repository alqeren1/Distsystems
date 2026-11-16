package main

import (
	"bufio"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"

	"google.golang.org/grpc"
)

func main() {
	maxbid := 0
	is_winning := false
	serverAddr := "localhost:50051"
	backupServerAddr := "localhost:50052"

	conn, err := grpc.NewBidder(serverAddr, grpc.WithInsecure())

	if err != nil {
		log.Fatalf("Component=Client Event=ConnectionSetupFailed Address=%s Error=%q", serverAddr, err)
	}

	defer conn.Close()

	fmt.Printf("Type a username: ")
	scanner := bufio.NewScanner(os.Stdin)
	bidderID := scanner.Text()

	for scanner.Scan() {
		text := strings.Split(scanner.Text(), " ")
		if text[0] == "bid" {
			current_bid, err := strconv.Atoi(text[1])
			if current_bid > 0 && err == nil {
				if current_bid > maxbid {
					maxbid = current_bid
					log.Printf("Component=Client Event=BidPlaced BidderID=%s Amount=%d", bidderID, current_bid)
					bid := &proto.BidderData{
						bidder_Id: bidderID,
						amount:    int32(current_bid),
					}
				} else {
					fmt.Printf("Bid must be greater than the current max bid of %d\n", maxbid)
				}
			} else {
				fmt.Printf("Invalid bid amount. Please enter a positive integer.\n")
			}
		} else if text[0] == "status" {
			log.Printf("Component=Client Event=StatusRequest BidderID=%s MaxBid=%d", bidderID, maxbid)
		}
	}
}
