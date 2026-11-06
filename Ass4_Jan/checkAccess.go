package main

import (
	"Ass4/proto"
	"context"
	"log"
)	


func (nm *NodeMemoryStruct) checkAccess(ctx context.Context, requesterID int64, requestTimestamp int64,) (*proto.ReplyAccessGranted, error)   {
	log.Printf("Node %d received request from node %d with timestamp %d", nm.nodeID, requesterID, requestTimestamp)
	
	nm.mu.Lock()
	// Update Lamport clock when receiving a message
    if requestTimestamp > nm.t {
        nm.t = requestTimestamp
    }
    nm.t++
	
	switch nm.state {
	case "released":
		log.Printf("Node %d is in released state, granting access to node %d", nm.nodeID, requesterID)
		nm.mu.Unlock()
		return &proto.ReplyAccessGranted{},nil
	case "held":
		log.Printf("Node %d is in held state, cannot grant access to node %d", nm.nodeID, requesterID)
		
	
	case "wanted":
		if requestTimestamp < nm.requestTime || (requestTimestamp == nm.requestTime && requesterID < nm.nodeID) {
			nm.mu.Unlock()
			return &proto.ReplyAccessGranted{},nil
		} else {
			log.Printf("Node %d is in wanted state, cannot grant access to node %d", nm.nodeID, requesterID)
			
	
	
		}

	default:
		nm.mu.Unlock()
		return nil,ctx.Err()
    
	}


	// If we reach here, the reply must be deferred
    waiter := deferredRequest{
        nodeID:    requesterID,
        timestamp: requestTimestamp,
        ready:     make(chan struct{}),
    }
	nm.waitQue = append(nm.waitQue, waiter)

	nm.mu.Unlock()

    // Wait until our node releases the Critical Section
    select {
		case <-waiter.ready:
			return &proto.ReplyAccessGranted{}, nil

		case <-ctx.Done():
			nm.mu.Lock()
			//If a request times out, its deferred entry should be removed from the queue.
			for i, req := range nm.waitQue {
				if req.ready == waiter.ready {
					nm.waitQue = append(
						nm.waitQue[:i],
						nm.waitQue[i+1:]...,
					)
					break
				}
			}

			nm.mu.Unlock()
}

    return nil, ctx.Err()
	
}