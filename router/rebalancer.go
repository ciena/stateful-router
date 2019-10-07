package router

import (
	"fmt"
	"github.com/khagerma/stateful-experiment/router/protos/peer"
	"math"
	"sort"
)

func (router *Router) startRebalancer() {
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
		router.peerMutex.RLock()
		sortedNodes := make([]uint32, 1, len(router.peers)+1)
		sortedNodes[0] = router.ordinal

		var totalDeviceCount = uint64(deviceCount)
		var nodeCount uint64 = 1

		shouldJumpToMax := true
		for nodeId, node := range router.peers {
			if node.connected {
				if node.maxReadiness || node.readiness > router.readiness {
					shouldJumpToMax = false
				}

				totalDeviceCount += uint64(node.devices)
				nodeCount++

				sortedNodes = append(sortedNodes, nodeId)
			}
		}

		// sort nodes by ID
		sort.Slice(sortedNodes, func(i, j int) bool { return sortedNodes[i] < sortedNodes[j] })

		// devices per peer, and remainder
		averageDevices := uint32(totalDeviceCount / nodeCount)
		remainingDevices := totalDeviceCount % nodeCount

		// calculate how many devices each peer should have
		nodesShouldHave := make(map[uint32]uint32, len(sortedNodes))
		for _, nodeId := range sortedNodes[0:remainingDevices] {
			nodesShouldHave[nodeId] = averageDevices + 1
		}
		for _, nodeId := range sortedNodes[remainingDevices:] {
			nodesShouldHave[nodeId] = averageDevices
		}

		increment, decrement := false, false

		// if too few devices
		if deviceCount < nodesShouldHave[router.ordinal] {
			// pull device
			increment = true
		}

		// if too many devices
		if deviceCount > nodesShouldHave[router.ordinal] {

			// iff ALL other nodes have >= correct number of devices OR have MAX readiness
			allPeersMeetRequisite := true
			for nodeId, node := range router.peers {
				if node.connected {
					if !(node.devices >= nodesShouldHave[nodeId] || node.maxReadiness) {
						allPeersMeetRequisite = false
						break
					}
				}
			}
			if allPeersMeetRequisite {
				// push device
				decrement = true
			}
		}
		router.peerMutex.RUnlock()

		if !router.maxReadiness && (shouldJumpToMax || increment) {
			// increase readiness
			proposedIncrementReadiness, proposedIncrementMaxReadiness := router.searchPeersForNextDevice()
			fmt.Printf("%d Increment: Changing readiness to %016x MAX:%v\n", router.ordinal, proposedIncrementReadiness, proposedIncrementMaxReadiness)
			router.changeReadinessTo(true, proposedIncrementReadiness, proposedIncrementMaxReadiness)
		} else if decrement {
			// decrease readiness
			fmt.Printf("%d Decrement: Changing readiness to %016x\n", router.ordinal, proposedDecrementReadiness)
			router.changeReadinessTo(false, proposedDecrementReadiness, false)

			// after readiness is decreased, kick out any devices that no longer belong on this node
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
		} else {
			// if readiness has stabilized, we don't need to repeat unless something changes
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

func (router *Router) searchPeersForNextDevice() (uint64, bool) {
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
// but it must be broadcast then updated when decreasing (have other nodes stop sending requests, then stop accepting requests)
// in addition, when decreasing from maxReadiness, a peer may reject the request, in which case other peers must be reverted
// (this is to ensure that at least one node always has maximum readiness, so that at least one node can handle any request)
func (router *Router) changeReadinessTo(increase bool, readiness uint64, maxReadiness bool) {
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
