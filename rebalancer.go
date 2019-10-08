package router

import (
	"fmt"
	"github.com/kent-h/stateful-router/protos/peer"
	"sort"
	"strconv"
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
				Ordinal:       router.ordinal,
				Readiness:     []byte(router.readiness),
				ReadyForEqual: router.readyForEqual,
				ReadinessMax:  router.readinessMax,
			}); err != nil {
				fmt.Println(err)
			}
		}

		router.deviceMutex.RLock()
		deviceCount := uint32(len(router.devices))
		var proposedDecrementReadiness string
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
				if node.readinessMax || node.readiness > router.readiness || (node.readiness == router.readiness && node.readyForEqual && !router.readyForEqual) {
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
					if !(node.devices >= nodesShouldHave[nodeId] || node.readinessMax) {
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

		if !router.readinessMax && (shouldJumpToMax || increment) {
			// increase readiness
			proposedIncrementReadiness, proposedIncrementMaxReadiness := router.searchPeersForNextDevice()
			if proposedIncrementMaxReadiness {
				fmt.Printf("%d Increment: Changing readiness to MAX\n", router.ordinal)
			} else {
				fmt.Printf("%d Increment: Changing readiness to ∀ <= %s\n", router.ordinal, strconv.Quote(proposedIncrementReadiness))
			}
			router.changeReadinessTo(true, proposedIncrementReadiness, true, proposedIncrementMaxReadiness)
		} else if decrement {
			// decrease readiness
			fmt.Printf("%d Decrement: Changing readiness to ∀ < %s\n", router.ordinal, strconv.Quote(proposedDecrementReadiness))
			router.changeReadinessTo(false, proposedDecrementReadiness, false, false)

			// after readiness is decreased, kick out any devices that no longer belong on this node
			router.deviceMutex.Lock()
			originalDeviceCount := uint32(len(router.devices))
			devicesToMove := make(map[string]*deviceData)
			if !router.readinessMax {
				for deviceId, device := range router.devices {
					//for every device that no longer belongs on this node
					if deviceId > router.readiness || (deviceId == router.readiness && !router.readyForEqual) {
						fmt.Printf("Will migrate device %s\n", strconv.Quote(deviceId))
						//release and notify that it's moved
						devicesToMove[deviceId] = device
						delete(router.devices, deviceId)
					}
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

func (router *Router) searchPeersForNextDevice() (string, bool) {
	router.peerMutex.Lock()
	toNotify := make(map[uint32]peer.PeerClient, len(router.peers))
	for nodeId, node := range router.peers {
		if node.connected {
			toNotify[nodeId] = node.PeerClient
		}
	}
	router.peerMutex.Unlock()

	// keep nodes informed on how ready we are
	next := ""
	have := false
	first, isOnlyDeviceToMigrate := true, false
	for _, client := range toNotify {
		// inform peers that we're ready for devices
		resp, err := client.NextDevice(router.ctx, &peer.NextDeviceRequest{
			Ordinal:       router.ordinal,
			Readiness:     []byte(router.readiness),
			ReadyForEqual: router.readyForEqual,
			ReadinessMax:  router.readinessMax})
		if err != nil {
			fmt.Println(err)
		} else {
			// determine which device is next in line for migration
			if resp.Has {
				have = true
				if first {
					first = false
					isOnlyDeviceToMigrate = resp.Last
					next = string(resp.Device)
				} else {
					isOnlyDeviceToMigrate = false
					if string(resp.Device) < next {
						next = string(resp.Device)
					}
				}
			}
		}
	}
	if isOnlyDeviceToMigrate {
		next = ""
	}
	return next, !have || isOnlyDeviceToMigrate
}

// changeReadinessTo handles the complexity of changing readiness
// readiness must be updated then broadcast when increasing (start accepting requests, then have other nodes start sending requests)
// but it must be broadcast then updated when decreasing (have other nodes stop sending requests, then stop accepting requests)
// in addition, when decreasing from readinessMax, a peer may reject the request, in which case other peers must be reverted
// (this is to ensure that at least one node always has maximum readiness, so that at least one node can handle any request)
func (router *Router) changeReadinessTo(increase bool, readiness string, readyForEqual, readinessMax bool) {
	router.peerMutex.Lock()
	if increase {
		// change which requests we will accept locally
		router.readiness, router.readyForEqual, router.readinessMax = readiness, readyForEqual, readinessMax
	} else {
		// if we are decreasing from readinessMax, we will need to be able to conditionally reject requests
		router.decreasingFromMaxReadiness = router.readinessMax && !readinessMax
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
			Ordinal:       router.ordinal,
			Readiness:     []byte(readiness),
			ReadyForEqual: readyForEqual,
			ReadinessMax:  readinessMax,
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
			Ordinal:       router.ordinal,
			Readiness:     []byte(router.readiness),
			ReadyForEqual: readyForEqual,
			ReadinessMax:  router.readinessMax,
		}); err != nil {
			fmt.Println(err)
		}
	}

	if (!increase && !abort) || router.decreasingFromMaxReadiness {

		router.peerMutex.Lock()
		if !increase && !abort {
			// change which requests we will accept locally
			router.readiness, router.readyForEqual, router.readinessMax = readiness, readyForEqual, readinessMax
		}
		if router.decreasingFromMaxReadiness {
			router.decreasingFromMaxReadiness = false
		}
		router.peerMutex.Unlock()
	}
}
