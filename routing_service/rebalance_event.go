package routing

import (
	"fmt"
	"github.com/khagerma/stateful-experiment/protos/peer"
	"sort"
)

type rebalanceEventData struct {
	ch       chan struct{}
	newPeers map[peer.PeerClient]struct{}
}

func (router *routingService) peerConnected(node *node) {
	router.eventMutex.Lock()
	defer router.eventMutex.Unlock()

	router.rebalanceEventData.newPeers[node] = struct{}{}
	if router.rebalanceEventData.ch != nil {
		close(router.rebalanceEventData.ch)
		router.rebalanceEventData.ch = nil
	}

	// update device count as well
	if router.deviceCountEventData.ch != nil {
		close(router.deviceCountEventData.ch)
		router.deviceCountEventData.ch = nil
	}
}

func (router *routingService) recalculateReadiness() {
	router.eventMutex.Lock()
	defer router.eventMutex.Unlock()

	if router.rebalanceEventData.ch != nil {
		close(router.rebalanceEventData.ch)
		router.rebalanceEventData.ch = nil
	}
}

func (router *routingService) startRebalancer() {
	defer close(router.rebalanceEventHandlerDone)
	for {
		router.eventMutex.Lock()
		ch := make(chan struct{})
		data := router.rebalanceEventData
		router.rebalanceEventData = rebalanceEventData{
			ch:       ch,
			newPeers: make(map[peer.PeerClient]struct{}),
		}
		router.eventMutex.Unlock()

		// send hello message to new peers
		for node := range data.newPeers {
			if _, err := node.UpdateReadiness(router.ctx, &peer.ReadinessRequest{
				Ordinal:      router.ordinal,
				Readiness:    router.readiness,
				MaxReadiness: router.maxReadiness,
			}); err != nil {
				fmt.Println(err)
			}
		}

		router.deviceMutex.RLock()
		deviceCount := uint32(len(router.devices))

		var proposedDecrementReadiness uint64
		for deviceId := range router.devices {
			if deviceId > proposedDecrementReadiness {
				proposedDecrementReadiness = deviceId
			}
		}
		router.deviceMutex.RUnlock()

		// we want to jump to max (accept all devices) if all nodes w/ > readiness are suddenly missing
		// we want to increment if devices < average # of devices on nodes w/ >= the proposed readiness
		// we want to decrement if devices > average # of devices on nodes w/ > the proposed readiness
		shouldJumpToMax := !router.maxReadiness
		var numPeers uint64 = 1
		var numGreaterOrdinalNodes uint64
		var peersDeviceCount = uint64(deviceCount)

		router.peerMutex.RLock()
		for nodeId, node := range router.peers {
			if node.connected {
				if node.maxReadiness || node.readiness > router.readiness {
					shouldJumpToMax = false
				}

				peersDeviceCount += uint64(node.devices)
				numPeers++
				if nodeId > router.ordinal {
					numGreaterOrdinalNodes++
				}
			}
		}

		sortedNodes := make([]uint32, len(router.peers)+1)
		sortedNodes[0] = router.ordinal
		ctr := 1
		for nodeId := range router.peers {
			sortedNodes[ctr] = nodeId
			ctr++
		}
		sort.Slice(sortedNodes, func(i, j int) bool { return sortedNodes[i] < sortedNodes[j] })

		// calculate how many devices each peer should have
		nodesShouldHave := make(map[uint32]uint32, len(sortedNodes))
		for i, nodeId := range sortedNodes {
			if nodeId != router.ordinal {
				nodesShouldHave[nodeId] = uint32(peersDeviceCount / numPeers)
				// if to the left of the shouldHave line
				if uint64(i) < peersDeviceCount%numPeers {
					nodesShouldHave[nodeId]++
				}
			}
		}
		fmt.Println(router.ordinal, "nodesShouldHave:", nodesShouldHave, peersDeviceCount, numPeers)

		maxDevices := uint32((peersDeviceCount + numGreaterOrdinalNodes) / numPeers)
		minDevices := int64(maxDevices)
		//if (peersDeviceCount+numGreaterOrdinalNodes)%numPeers == 0 {
		//	minDevices-- // ?
		//}
		fmt.Println(router.ordinal, "Have", deviceCount, "should have between", minDevices, "and", maxDevices)

		increment, decrement := false, false

		// if too few devices
		if int64(deviceCount) < minDevices {
			// pull
			increment = true
		}

		// if too many devices
		if deviceCount > maxDevices {
			// iff ALL other nodes have >= correct number of devices OR have MAX readiness
			allPeersMeetRequisite := true
			for nodeId, node := range router.peers {
				if !(node.devices >= nodesShouldHave[nodeId] || node.maxReadiness) {
					fmt.Println(router.ordinal, "peer", nodeId, "doesn't meet requisite, has", node.devices)
					allPeersMeetRequisite = false
					break
				}
			}
			fmt.Println(router.ordinal, "allPeersMeetRequisite:", allPeersMeetRequisite)
			if allPeersMeetRequisite {
				// push
				decrement = true
			}
		}
		router.peerMutex.RUnlock()

		// repeat flag for readiness change
		done := false

		if !router.maxReadiness && (shouldJumpToMax || increment) {
			proposedIncrementReadiness, proposedIncrementMaxReadiness := router.searchPeersForNextDevice()
			fmt.Println(router.ordinal, "Increment: Changing readiness to", proposedIncrementReadiness, proposedIncrementMaxReadiness)
			router.changeReadinessTo(true, proposedIncrementReadiness, proposedIncrementMaxReadiness)
		} else if router.readiness != 0 && decrement {
			fmt.Println(router.ordinal, "Decrement: Changing readiness to", proposedDecrementReadiness, false)
			router.changeReadinessTo(false, proposedDecrementReadiness, false)
		} else {
			// we're done, and don't need to repeat unless something changes
			done = true
		}

		// if readiness has decreased, kick out any devices that no longer belong on this node
		router.deviceMutex.Lock()
		originalDeviceCount := uint32(len(router.devices))
		devicesToMove := make(map[uint64]*deviceData)
		for deviceId, device := range router.devices {
			//for every device that no longer belongs on this node
			if deviceId >= router.readiness && !router.maxReadiness {
				fmt.Println("will migrate device", deviceId)
				//release and notify that it's moved
				devicesToMove[deviceId] = device
				delete(router.devices, deviceId)
			}
		}
		router.deviceMutex.Unlock()

		router.migrateDevices(devicesToMove, originalDeviceCount)

		if done {
			// wait until something changes
			select {
			case <-router.ctx.Done():
				return
			case <-ch:
				// event received
			}
		}
	}
}
