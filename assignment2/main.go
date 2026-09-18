package main

import (
	"fmt"
	"math/rand"
	"os"
	"time"
)

// the TCPcall with necessary info
type TCP struct {
	SYN   bool // bool because real TCP uses a bit.
	ACK   bool
	seqNr int32 // int32 because that's the size in a real TCP
	ackNr int32
}

func process(name string, TCP_handshake_ch chan TCP) {

	// One function to create ether a client or a server
	switch {
	case name == "client":
		var baseSeqNr int32 = 100                  // First Sequence number to send
		TCPsyn := TCP{SYN: true, seqNr: baseSeqNr} // The SYN sent from client to server
		TCP_handshake_ch <- TCPsyn
		fmt.Println(name, "sent SYN to server")
		TCPsynAck := <-TCP_handshake_ch                                          // Waiting to receive a SYN-ACK from server
		if TCPsynAck.SYN && TCPsynAck.ACK && TCPsynAck.ackNr == TCPsyn.seqNr+1 { // Checking that received info is correct
			fmt.Println(name, "received SYN-ACK from server")
		} else {
			fmt.Println(name, "received unvalid TCP")
		}
		TCPack := TCP{SYN: false, ACK: true, seqNr: TCPsynAck.ackNr, ackNr: TCPsynAck.seqNr + 1} // The ACK sent from client to server
		TCP_handshake_ch <- TCPack
		fmt.Println(name, "sent ACK to server")

	case name == "server":
		var baseSeqNr int32 = 300 // First Sequence number to send
		TCPreturn := TCP{false, false, 1, 2}
		for {

			TCPreceived := <-TCP_handshake_ch
			fmt.Println(name, "received SYN from client")
			switch {
			case TCPreceived.SYN && !TCPreceived.ACK: // if received SYN is true and ACK is false
				//	baseAckNr = TCPreceived.seqNr + 1
				TCPreturn = TCP{SYN: true, ACK: true, seqNr: int32(baseSeqNr), ackNr: TCPreceived.seqNr + 1}
				TCP_handshake_ch <- TCPreturn
				fmt.Println(name, "sent SYN-ACK to client")
			case !TCPreceived.SYN && TCPreceived.ACK && TCPreceived.seqNr == TCPreturn.ackNr && TCPreceived.ackNr == TCPreturn.seqNr+1: // if received SYN is false and ACK is true and Sequence number == last sent ACK number and ASCK number == last sent Sequence number + 1
				fmt.Println(name, "received ACK from client")
				fmt.Println("3-way Handshake established")
				os.Exit(0)
			default:
				fmt.Println(name, "received unvalid TCP")
				os.Exit(0)
			}

		}
	}

}

func middleware(client_ch chan TCP, server_ch chan TCP) {
	for {
		ran := rand.Intn(10)
		select {
		case client_answer := <-client_ch:
			if ran == 0 {
				fmt.Println("Middleware: Dropped packet from client")
				server_ch <- TCP{false, false, 0, 0}
			} else if ran == 1 {
				fmt.Println("Middleware: Delayed packet from client")
				time.Sleep(2 * time.Second)
				server_ch <- client_answer
			} else {
				fmt.Println("Middleware: Forwarded packet from client")
				server_ch <- client_answer
			}
		case server_answer := <-server_ch:
			if ran == 0 {
				fmt.Println("Middleware: Dropped packet from server")
				client_ch <- TCP{false, false, 0, 0}
			} else if ran == 1 {
				fmt.Println("Middleware: Delayed packet from server")
				time.Sleep(2 * time.Second)
				client_ch <- server_answer
			} else {
				fmt.Println("Middleware: Forwarded packet from server")
				client_ch <- server_answer
			}
		}
	}
}
func main() {
	TCP_server_ch := make(chan TCP)
	TCP_client_ch := make(chan TCP)

	go process("client", TCP_client_ch)

	go process("server", TCP_server_ch)

	go middleware(TCP_client_ch, TCP_server_ch)
	for {

	}
}
