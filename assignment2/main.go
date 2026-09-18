package main

import (
	"fmt"
	"os"
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

func main() {
	TCP_handshake_ch := make(chan TCP)

	go process("client", TCP_handshake_ch)

	go process("server", TCP_handshake_ch)
	for {

	}
}
