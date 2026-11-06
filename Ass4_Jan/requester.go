package main

import (
	"Ass4/proto"
	"context"
	"log"
	"sync"
	"time"
)

func (node *NodeMemoryStruct) beginRequest() (int64, bool) { //returns timestamp of new req and if its successfull

    // Lock shared memory
    node.mu.Lock()
    defer node.mu.Unlock()

    // Only request if currently RELEASED
    if node.state != "released" {
        return 0, false
    }

    // Increment Lamport clock
    node.t++

    // Save timestamp for this particular request
    node.requestTime = node.t

    // Change state
    node.state = "wanted"

    log.Printf(
        "Node %d: REQUESTING CS, timestamp=%d",
        node.nodeID,
        node.requestTime,
    )

    return node.requestTime, true
}

func (node *NodeMemoryStruct) requestPermissions(timestamp int64) bool {

    var wg sync.WaitGroup

    // One result for every peer
    results := make(chan error, len(node.clients))

    for _, peer := range node.clients {

        wg.Add(1)

        go func(p peerClient) {
            defer wg.Done()

            log.Printf(
                "Node %d sending REQUEST to Node %d",
                node.nodeID,
                p.id,
            )

            ctx, cancel := context.WithTimeout(
				context.Background(),
				60*time.Second,
			)
			defer cancel()

			_, err := p.client.RequestAccess(
				ctx,
				&proto.ReqData{
					NodeId: node.nodeID,
					T:      timestamp,
				},
			)

            if err != nil {
                log.Printf(
                    "Request to Node %d failed: %v",
                    p.id,
                    err,
                )
            } else {
                log.Printf(
                    "Node %d received REPLY from Node %d",
                    node.nodeID,
                    p.id,
                )
            }

            results <- err

        }(peer)
    }

    // Wait until all requests have finished
    wg.Wait()

    close(results)

    // Check whether every peer granted permission
    allGranted := true

    for err := range results {
        if err != nil {
            allGranted = false
        }
    }

    return allGranted
}