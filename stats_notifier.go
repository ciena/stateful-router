package router

import (
	"fmt"
	"github.com/kent-h/stateful-router/protos/peer"
)

func (router *Router) startStatsNotifier() {
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
