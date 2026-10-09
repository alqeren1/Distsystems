package main

import (
	"context"
	"fmt"
	"log"
	"time"
	"string"
	"bufio"
	"os"

	"chitchat/chitchat/grpc/proto"
	proto "chitchat/grpc/proto"


)
type Client struct {
	pb.UnimplementedChitchatClient
	clientID string
}

func NewClient(clientID string) *Client {
	return &Client{
		clientID: clientID,
	}
}


func main() {
	scan := bufio.NewScanner(os.Stdin)
	fmt.Println("Enter your client ID:")
	clientID := scan.Text()
	client := NewClient(clientID)
	go Scanner(scan, client)
}

func Scanner(scan *bufio.Scanner, client *Client) {
	for {
		message := scan.Text()
		_type := string.Fields(message)[0]
		if  _type == "join" {
			client.Join()
		} else if _type == "leave" {
			client.Leave()
		}else{
			client.SendMessage(message)
		}
}

func streamer(stream proto.Chitchat_JoinClient) {
	for {
		fmt.Println(stream.Recv())
	}
}
func (c *Client) Join(context context.Context, stream proto.Chitchat_JoinClient) error {
stream, err := c.Join(context, &proto.JoinReq{ClientId: c.clientID})
if err != nil {
	log.Fatalf("Failed to join: %v", err)
}
go streamer(stream)
}

func (c *Client) Leave() {}

func (c *Client) SendMessage(message string) {}