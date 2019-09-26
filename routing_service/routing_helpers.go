package routing

import (
	"context"
	"fmt"
	"github.com/khagerma/stateful-experiment/protos/peer"
	"math"
)

func (router *routingService) changeReadinessTo(readiness uint64) {
	router.peerMutex.Lock()
	// increment requests that we will accept locally
	router.readiness = readiness

	toNotify := make([]peer.PeerClient, 0, len(router.peers))
	for _, node := range router.peers {
		if node.connected {
			toNotify = append(toNotify, node.PeerClient)
		}
	}
	router.peerMutex.Unlock()

	// inform peers of our readiness
	for _, client := range toNotify {
		if _, err := client.UpdateReadiness(context.Background(), &peer.ReadinessRequest{Ordinal: router.ordinal, Readiness: readiness}); err != nil {
			fmt.Println(err)
		}
	}
}

func (router *routingService) searchPeersForNextDevice(readiness uint64) (uint64, bool, bool) {
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
		resp, err := client.NextDevice(context.Background(), &peer.NextDeviceRequest{Ordinal: router.ordinal, Readiness: readiness})
		if err == nil {
			// determine which device is next in line for migration
			if resp.Has {
				have = true
				if first {
					first = false
					isOnlyDeviceToMigrate = resp.Last
				}else{
					isOnlyDeviceToMigrate = false
				}
				if resp.Device <= next {
					next = resp.Device
				}
			}
		}
	}
	if isOnlyDeviceToMigrate {
		next = math.MaxUint64
	}
	return next, have, isOnlyDeviceToMigrate
}
