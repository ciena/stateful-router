package routing

import (
	"fmt"
	"github.com/khagerma/stateful-experiment/protos/peer"
)

type deviceCountEventData struct {
	ch             chan struct{}
	updateComplete chan struct{}
	updatingPeers  map[uint32]uint32
}

var closedChan chan struct{}

func init() {
	closedChan = make(chan struct{})
	close(closedChan)
}

func (router *routingService) deviceCountChanged() {
	router.eventMutex.Lock()
	defer router.eventMutex.Unlock()

	// rebalance as well
	if router.rebalanceEventData.ch != nil {
		close(router.rebalanceEventData.ch)
		router.rebalanceEventData.ch = nil
	}

	if router.deviceCountEventData.ch != nil {
		close(router.deviceCountEventData.ch)
		router.deviceCountEventData.ch = nil
	}
}

func (router *routingService) deviceCountPeerChanged(nodeId uint32, devices uint32) chan struct{} {
	router.eventMutex.Lock()
	defer router.eventMutex.Unlock()

	// rebalance as well
	if router.rebalanceEventData.ch != nil {
		close(router.rebalanceEventData.ch)
		router.rebalanceEventData.ch = nil
	}

	router.deviceCountEventData.updatingPeers[nodeId] = devices
	if router.deviceCountEventData.ch != nil {
		close(router.deviceCountEventData.ch)
		router.deviceCountEventData.ch = nil
	}

	return router.deviceCountEventData.updateComplete
}

func (router *routingService) startStatsNotifier() {
	defer close(router.deviceCountEventHandlerDone)
	for {
		router.eventMutex.Lock()
		ch := make(chan struct{})
		data := router.deviceCountEventData
		router.deviceCountEventData = deviceCountEventData{
			ch:             ch,
			updateComplete: make(chan struct{}),
			updatingPeers:  make(map[uint32]uint32),
		}
		router.eventMutex.Unlock()

		router.deviceMutex.RLock()
		deviceCount := uint32(len(router.devices))
		router.deviceMutex.RUnlock()

		// inform peers of changes to device counts
		router.peerMutex.RLock()
		//update ourselves
		for nodeId, devices := range data.updatingPeers {
			if node, have := router.peers[nodeId]; have {
				node.devices = devices
			}
		}
		//update others
		toUpdate := make(map[*node]struct{}, len(router.peers))
		for _, node := range router.peers {
			if node.connected {
				toUpdate[node] = struct{}{}
			}
		}
		router.peerMutex.RUnlock()

		data.updatingPeers[router.ordinal] = deviceCount
		for node := range toUpdate {
			if _, err := node.UpdateStats(router.ctx, &peer.StatsRequest{DeviceCounts: data.updatingPeers}); err != nil {
				fmt.Println(err)
			}
		}

		close(data.updateComplete)

		select {
		case <-router.ctx.Done():
			// anyone waiting for us can exit
			close(router.deviceCountEventData.updateComplete)
			return
		case <-ch: // on event, repeat
		}
	}
}
