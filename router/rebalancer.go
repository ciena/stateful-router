package routing

import (
	"fmt"
	"github.com/khagerma/stateful-experiment/protos/peer"
	"math"
	"sort"
)

func (router *router) startRebalancer() {
	fmt.Println("Starting rebalancer")
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
		// fmt.Println(router.ordinal, "nodesShouldHave:", nodesShouldHave, peersDeviceCount, numPeers)

		maxDevices := uint32((peersDeviceCount + numGreaterOrdinalNodes) / numPeers)
		minDevices := int64(maxDevices)
		//if (peersDeviceCount+numGreaterOrdinalNodes)%numPeers == 0 {
		//	minDevices-- // ?
		//}
		// fmt.Println(router.ordinal, "Have", deviceCount, "should have between", minDevices, "and", maxDevices)

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
					// fmt.Println(router.ordinal, "peer", nodeId, "doesn't meet requisite, has", node.devices)
					allPeersMeetRequisite = false
					break
				}
			}
			// fmt.Println(router.ordinal, "allPeersMeetRequisite:", allPeersMeetRequisite)
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
			fmt.Printf("%d Increment: Changing readiness to %016x MAX:%v\n", router.ordinal, proposedIncrementReadiness, proposedIncrementMaxReadiness)
			router.changeReadinessTo(true, proposedIncrementReadiness, proposedIncrementMaxReadiness)
		} else if router.readiness != 0 && decrement {
			fmt.Printf("%d Decrement: Changing readiness to %016x\n", router.ordinal, proposedDecrementReadiness)
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
				fmt.Printf("Will migrate device %016x\n", deviceId)
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

func (router *router) searchPeersForNextDevice() (uint64, bool) {
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

// changeReadinessTo handles the complexity of changing readiness
// readiness must be updated then broadcast when increasing (start accepting requests, then have other nodes start sending requests)
// but it must be broadcast then updated when decreasing (have other nodes stop sending requests, the stop accepting requests)
// in addition, when decreasing from maxReadiness, a peer may reject the request, in which case other peers must be reverted
// (this is to ensure that at least one node always has maximum readiness, so that at least one node can handle any request)
func (router *router) changeReadinessTo(increase bool, readiness uint64, maxReadiness bool) {
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
