package routing

import (
	"fmt"
	"github.com/khagerma/stateful-experiment/protos/peer"
	"math"
)

// changeReadinessTo handles the complexity of changing readiness
// readiness must be updated then broadcast when increasing (start accepting requests, then have other nodes start sending requests)
// but it must be broadcast then updated when decreasing (have other nodes stop sending requests, the stop accepting requests)
// in addition, when decreasing from maxReadiness, a peer may reject the request, in which case other peers must be reverted
// (this is to ensure that at least one node always has maximum readiness, so that at least one node can handle any request)
func (router *routingService) changeReadinessTo(increase bool, readiness uint64, maxReadiness bool) {
	router.peerMutex.Lock()
	if increase {
		// change which requests we will accept locally
		router.readiness, router.maxReadiness = readiness, maxReadiness
	} else {
		// if we are decreasing from maxReadiness, we will need to be able to conditionally reject requests
		router.decreasingFromMaxReadiness = router.maxReadiness && !maxReadiness
	}

	toNotify := make([]peer.PeerClient, 0, len(router.peers))
	for _, node := range router.peers {
		if node.connected {
			toNotify = append(toNotify, node.PeerClient)
		}
	}
	router.peerMutex.Unlock()

	// inform peers of our readiness
	abort, undo := false, 0
	for i, client := range toNotify {
		if _, err := client.UpdateReadiness(router.ctx, &peer.ReadinessRequest{
			Ordinal:      router.ordinal,
			Readiness:    readiness,
			MaxReadiness: maxReadiness,
		}); err != nil {
			if err.Error() == "rpc error: code = Unknown desc = no other nodes exist with max readiness" {
				fmt.Println("known error code, changing from", router.readiness, router.maxReadiness, "to", readiness, maxReadiness, increase)
				abort, undo = true, i
			}
			fmt.Println(err)
		}
	}

	// if aborted, revert notified peers
	for _, client := range toNotify[0:undo] {
		if _, err := client.UpdateReadiness(router.ctx, &peer.ReadinessRequest{
			Ordinal:      router.ordinal,
			Readiness:    router.readiness,
			MaxReadiness: router.maxReadiness,
		}); err != nil {
			fmt.Println(err)
		}
	}

	if (!increase && !abort) || router.decreasingFromMaxReadiness {

		router.peerMutex.Lock()
		if !increase && !abort {
			// change which requests we will accept locally
			router.readiness, router.maxReadiness = readiness, maxReadiness
		}
		if router.decreasingFromMaxReadiness {
			router.decreasingFromMaxReadiness = false
		}
		router.peerMutex.Unlock()
	}
}

func (router *routingService) searchPeersForNextDevice() (uint64, bool) {
	router.peerMutex.Lock()
	toNotify := make(map[uint32]peer.PeerClient, len(router.peers))
	for nodeId, node := range router.peers {
		if node.connected {
			toNotify[nodeId] = node.PeerClient
		}
	}
	router.peerMutex.Unlock()

	// keep nodes informed on how ready we are
	next := uint64(math.MaxUint64)
	have := false
	first, isOnlyDeviceToMigrate := true, false
	for _, client := range toNotify {
		// inform peers that we're ready for devices
		resp, err := client.NextDevice(router.ctx, &peer.NextDeviceRequest{
			Ordinal:      router.ordinal,
			Readiness:    router.readiness,
			MaxReadiness: router.maxReadiness})
		if err != nil {
			fmt.Println(err)
		} else {
			// determine which device is next in line for migration
			if resp.Has {
				have = true
				if first {
					first = false
					isOnlyDeviceToMigrate = resp.Last
				} else {
					isOnlyDeviceToMigrate = false
				}
				if resp.Device <= next {
					next = math.MaxUint64
					if resp.Device != math.MaxUint64 {
						next = resp.Device + 1
					}
				}
			}
		}
	}
	if isOnlyDeviceToMigrate {
		next = math.MaxUint64
	}
	return next, !have || isOnlyDeviceToMigrate
}
