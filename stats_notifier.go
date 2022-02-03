package router

import (
	"fmt"
	"github.com/kent-h/stateful-router/protos/peer"
)

func (router *Router) startStatsNotifier() {
	defer close(router.statusNotifierDone)
	for {
		router.eventMutex.Lock()
		ch := make(chan struct{})
		data := router.resourceCountEventData
		router.resourceCountEventData = resourceCountEventData{
			ch:             ch,
			updateComplete: make(chan struct{}),
			resources:      make(map[ResourceType]map[uint32]uint32),
		}
		router.eventMutex.Unlock()

		// generate the list of updates
		resourceStats := make(map[uint32]*peer.NodeStats, len(data.resources))
		for resourceType, updatingForPeers := range data.resources {
			update := &peer.NodeStats{}
			// add stats from self
			resource := router.resources[resourceType]

			resource.mutex.RLock()
			count := uint32(len(resource.loaded))
			resource.mutex.RUnlock()

			update.Stats = append(update.Stats, &peer.NodeStat{
				Ordinal: router.ordinal,
				Count:   count,
			})

			// add stats from peers
			for nodeId, count := range updatingForPeers {
				update.Stats = append(update.Stats, &peer.NodeStat{
					Ordinal: nodeId,
					Count:   count,
				})
			}

			resourceStats[uint32(resourceType)] = update
		}

		// inform peers of changes to device counts
		router.peerMutex.Lock()
		//update ourselves
		for resourceType, updatingForPeers := range data.resources {
			for nodeId, devices := range updatingForPeers {
				if node, have := router.peers[nodeId]; have {
					nodeResource := node.resources[resourceType]
					nodeResource.count = devices
					node.resources[resourceType] = nodeResource
				}
			}
			if len(updatingForPeers) != 0 {
				router.rebalance(resourceType)
			}
		}

		// get the list of peers
		toUpdate := make(map[*node]struct{}, len(router.peers))
		for _, node := range router.peers {
			if node.connected {
				toUpdate[node] = struct{}{}
			}
		}
		router.peerMutex.Unlock()

		// send updates to peers
		for node := range toUpdate {
			if _, err := node.UpdateStats(router.ctx, &peer.StatsRequest{ResourceStats: resourceStats}); err != nil {
				fmt.Println("unable to update stats:", err)
			}
		}

		close(data.updateComplete)

		select {
		case <-router.ctx.Done():
			// anyone waiting for us can exit
			close(router.resourceCountEventData.updateComplete)
			return
		case <-ch: // on event, repeat
		}
	}
}
