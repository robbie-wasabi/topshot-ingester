package main

import (
	"context"
	"fmt"
	"time"

	"github.com/rrrkren/topshot-sales/topshot"

	"github.com/onflow/flow-go-sdk/client"
	"google.golang.org/grpc"
)

const (
	MAX_STEP_SIZE = 200
	STEP_TIME_MS  = 1000
)

func handleErr(err error) {
	if err != nil {
		panic(err)
	}
}

func main() {

	blockHeightCursor := 14825664

	for {
		blocksReadCount := ingestEvents(uint64(blockHeightCursor))
		blockHeightCursor = blockHeightCursor + blocksReadCount
		time.Sleep(STEP_TIME_MS * time.Millisecond)
	}
}

func ingestEvents(blockHeightCursor uint64) int {
	// connect to flow
	flowClient, err := client.New("access.mainnet.nodes.onflow.org:9000", grpc.WithInsecure())
	handleErr(err)
	err = flowClient.Ping(context.Background())
	handleErr(err)

	latestBlock, err := flowClient.GetLatestBlock(context.Background(), true)
	if err != nil {
		panic(err)
	}
	latestBlockHeight := latestBlock.Height

	step := int(latestBlockHeight) - int(blockHeightCursor)
	if step == 0 {
		fmt.Println("cursor reached latest block: ", latestBlockHeight)
		return int(step)
	} else if step > MAX_STEP_SIZE {
		step = MAX_STEP_SIZE
	}

	endBlockHeight := blockHeightCursor + uint64(step)

	// fetch block events of topshot Market.MomentPurchased events for the past 1000 blocks
	blockEvents, err := flowClient.GetEventsForHeightRange(context.Background(), client.EventRangeQuery{
		Type:        "A.c1e4f4f4c4257510.Market.MomentPurchased",
		StartHeight: blockHeightCursor,
		EndHeight:   endBlockHeight,
	})
	handleErr(err)

	for _, blockEvent := range blockEvents {
		for _, purchaseEvent := range blockEvent.Events {
			// loop through the Market.MomentPurchased events in this blockEvent
			e := topshot.MomentPurchasedEvent(purchaseEvent.Value)
			fmt.Println(e)
			saleMoment, err := topshot.GetSaleMomentFromOwnerAtBlock(flowClient, blockEvent.Height-1, *e.Seller(), e.Id())
			handleErr(err)
			fmt.Println(saleMoment)
			fmt.Printf("transactionID: %s, block height: %d\n",
				purchaseEvent.TransactionID.String(), blockEvent.Height)
			fmt.Println()
		}
	}

	return int(step)
}
