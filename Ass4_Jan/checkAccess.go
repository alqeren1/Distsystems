	package main

import (
		"Ass4/proto"
		"bufio"
		"context"
		"flag"
		"log"
		"net"
		"os"	

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	)	


func (nm *NodeMemoryStruct) checkAccess(ctx context.Context, req *proto.ReqData) *proto.ReplyAccessGranted   {
	log.Printf("Node %d received request from node %d with timestamp %d", nm.nodeID, req.NodeId, req.T)
	mutex.Lock()

	switch nm.state {
	case "released":
		log.Printf("Node %d is in released state, granting access to node %d", nm.nodeID, req.NodeId)
		nm.state = "held"
		return &proto.ReplyAccessGranted{}, nil
	case "held":
		log.Printf("Node %d is in held state, cannot grant access to node %d", nm.nodeID, req.NodeId)
		return nil, nil
	case "wanted":
		if req.T < nm.t || (req.T == nm.t && req.NodeId < nm.nodeID) {
			nm.state = "held"
			return &proto.ReplyAccessGranted{}, nil
		} else {
			log.Printf("Node %d is in wanted state, cannot grant access to node %d", nm.nodeID, req.NodeId)
			return nil, nil
		}
	}
	mutex.Unlock()
}