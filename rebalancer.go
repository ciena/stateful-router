package router

import (
	"context"
	"fmt"
	"github.com/kent-h/stateful-router/protos/peer"
	"sort"
	"strconv"
)

func (router *Router) startRebalancer(handoffAndShutdown chan struct{}, ctxCancelFunc context.CancelFunc) {
	fmt.Println("Starting rebalancer")
	defer ctxCancelFunc()

	router.rebalanceAll()
	startingUp, shuttingDown := true, false
	for {
		router.eventMutex.Lock()
		ch := make(chan struct{})
		data := router.rebalanceEventData
		router.rebalanceEventData = rebalanceEventData{
			ch:            ch,
			newPeers:      make(map[peer.PeerClient]struct{}),
			resourceTypes: make(map[ResourceType]struct{}),
		}
		router.eventMutex.Unlock()

		readiness := make([]*peer.Readiness, len(router.resources))
		ctr := 0
		for resourceType, resource := range router.resources {
			// update readiness of any new peers
			readiness[ctr] = &peer.Readiness{
				ResourceType:  uint32(resourceType),
				Readiness:     []byte(resource.readiness),
				ReadyForEqual: resource.readyForEqual,
				Max:           resource.readinessMax,
			}
			ctr++
		}

		for node := range data.newPeers {
			if _, err := node.UpdateReadiness(router.ctx, &peer.ReadinessRequest{
				Ordinal:      router.ordinal,
				Readiness:    readiness,
				ShuttingDown: shuttingDown,
			}); err != nil {
				fmt.Println("unable to update new peer's readiness:", err)
			}
		}

		if startingUp || shuttingDown {
			startingUp = false
			// at startup and shutdown, rebalance every resource
			for resourceType := range router.resources {
				data.resourceTypes[resourceType] = struct{}{}
			}
		}

		for len(data.resourceTypes) != 0 {
			for resourceType := range data.resourceTypes {
				localResource := router.resources[resourceType]

				localResource.deviceMutex.RLock()
				deviceCount := uint32(len(localResource.devices))
				var proposedDecrementReadiness string
				for deviceId := range localResource.devices {
					if deviceId > proposedDecrementReadiness {
						proposedDecrementReadiness = deviceId
					}
				}
				localResource.deviceMutex.RUnlock()

				// we want to jump to max (accept all resources) if all nodes w/ > readiness are suddenly missing
				// we want to increment if resources < average # of resources on nodes w/ >= the proposed readiness
				// we want to decrement if resources > average # of resources on nodes w/ > the proposed readiness
				router.peerMutex.RLock()
				sortedNodes := make([]uint32, 1, len(router.peers)+1)
				sortedNodes[0] = router.ordinal

				var totalDeviceCount = uint64(deviceCount)
				var nodeCount uint64 = 1

				shouldJumpToMin := shuttingDown && (localResource.readinessMax || localResource.readiness != "" || localResource.readyForEqual)
				shouldJumpToMax := true
				for nodeId, node := range router.peers {
					if node.connected && !node.shuttingDown {
						nodeResource := node.resources[resourceType]
						if nodeResource.readinessMax || nodeResource.readiness > localResource.readiness || (nodeResource.readiness == localResource.readiness && nodeResource.readyForEqual && !localResource.readyForEqual) {
							shouldJumpToMax = false
						}

						totalDeviceCount += uint64(nodeResource.count)
						nodeCount++

						sortedNodes = append(sortedNodes, nodeId)
					}
				}

				// sort nodes by ID
				sort.Slice(sortedNodes, func(i, j int) bool { return sortedNodes[i] < sortedNodes[j] })

				// resources per peer, and remainder
				averageDevices := uint32(totalDeviceCount / nodeCount)
				remainingDevices := totalDeviceCount % nodeCount

				// calculate how many resources each peer should have
				nodesShouldHave := make(map[uint32]uint32, len(sortedNodes))
				for _, nodeId := range sortedNodes[0:remainingDevices] {
					nodesShouldHave[nodeId] = averageDevices + 1
				}
				for _, nodeId := range sortedNodes[remainingDevices:] {
					nodesShouldHave[nodeId] = averageDevices
				}

				increment, decrement := false, false

				// if too few resources
				if deviceCount < nodesShouldHave[router.ordinal] {
					// pull device
					increment = true
				}

				// if too many resources
				if deviceCount > nodesShouldHave[router.ordinal] {

					// iff ALL other nodes have >= correct number of resources OR have MAX readiness
					allPeersMeetRequisite := true
					for nodeId, node := range router.peers {
						if node.connected && !node.shuttingDown {
							if nodeData := node.resources[resourceType]; !(nodeData.count >= nodesShouldHave[nodeId] || nodeData.readinessMax) {
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

				if !shuttingDown && !localResource.readinessMax && (shouldJumpToMax || increment) {
					// increase readiness
					proposedIncrementReadiness, proposedIncrementMaxReadiness := router.searchPeersForNextResource(resourceType)
					if proposedIncrementMaxReadiness {
						fmt.Printf("%d Increment: Changing readiness to MAX\n", router.ordinal)
					} else {
						fmt.Printf("%d Increment: Changing readiness to ∀ <= %s\n", router.ordinal, strconv.Quote(proposedIncrementReadiness))
					}
					router.changeReadinessTo(resourceType, true, proposedIncrementReadiness, true, proposedIncrementMaxReadiness, false)
				} else if shouldJumpToMin || decrement {
					// decrease readiness
					fmt.Printf("%d Decrement: Changing readiness to ∀ < %s\n", router.ordinal, strconv.Quote(proposedDecrementReadiness))
					router.changeReadinessTo(resourceType, false, proposedDecrementReadiness, false, false, shuttingDown)

					// after readiness is decreased, kick out any resources that no longer belong on this node
					localResource.deviceMutex.Lock()
					originalDeviceCount := uint32(len(localResource.devices))
					devicesToMove := make(map[string]*deviceData)
					if !localResource.readinessMax {
						for deviceId, device := range localResource.devices {
							//for every device that no longer belongs on this node
							if deviceId > localResource.readiness || (deviceId == localResource.readiness && !localResource.readyForEqual) {
								fmt.Printf("Will migrate device %s\n", strconv.Quote(deviceId))
								//release and notify that it's moved
								devicesToMove[deviceId] = device
								delete(localResource.devices, deviceId)
							}
						}
					}
					localResource.deviceMutex.Unlock()

					router.migrateResources(resourceType, devicesToMove, originalDeviceCount)
				} else {
					delete(data.resourceTypes, resourceType)
				}
			}
		}
		// if readiness has stabilized, we don't need to repeat unless something changes
		// wait until something changes
		select {
		case <-handoffAndShutdown:
			if !shuttingDown {
				// run the rebalancer once more, to cleanly unload all resources
				shuttingDown = true
			} else {
				return
			}
		case <-ch:
			// event received
		}
	}
}

func (router *Router) searchPeersForNextResource(resourceType ResourceType) (string, bool) {
	router.peerMutex.Lock()
	toNotify := make(map[uint32]peer.PeerClient, len(router.peers))
	for nodeId, node := range router.peers {
		if node.connected {
			toNotify[nodeId] = node.PeerClient
		}
	}
	router.peerMutex.Unlock()

	resource := router.resources[resourceType]

	// keep nodes informed on how ready we are
	next := ""
	have := false
	first, isOnlyDeviceToMigrate := true, false
	for _, client := range toNotify {
		// inform peers that we're ready for resources
		resp, err := client.NextResource(router.ctx, &peer.NextResourceRequest{
			Ordinal:       router.ordinal,
			ResourceType:  uint32(resourceType),
			Readiness:     []byte(resource.readiness),
			ReadyForEqual: resource.readyForEqual,
			ReadinessMax:  resource.readinessMax})
		if err != nil {
			fmt.Println("unable to get next resource from peer:", err)
		} else {
			// determine which device is next in line for migration
			if resp.Has {
				have = true
				if first {
					first = false
					isOnlyDeviceToMigrate = resp.Last
					next = string(resp.ResourceId)
				} else {
					isOnlyDeviceToMigrate = false
					if string(resp.ResourceId) < next {
						next = string(resp.ResourceId)
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
func (router *Router) changeReadinessTo(resourceType ResourceType, increase bool, readiness string, readyForEqual, readinessMax, shuttingDown bool) {
	resource := router.resources[resourceType]

	router.peerMutex.Lock()
	if increase {
		// change which requests we will accept locally
		resource.readiness, resource.readyForEqual, resource.readinessMax = readiness, readyForEqual, readinessMax
	} else {
		// if we are decreasing from readinessMax, we will need to be able to conditionally reject requests
		resource.decreasingFromMaxReadiness = resource.readinessMax && !readinessMax
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
			Ordinal: router.ordinal,
			Readiness: []*peer.Readiness{{
				ResourceType:  uint32(resourceType),
				Readiness:     []byte(readiness),
				ReadyForEqual: readyForEqual,
				Max:           readinessMax,
			}},
			ShuttingDown: shuttingDown,
		}); err != nil {
			if err.Error() == "rpc error: code = Unknown desc = no other nodes exist with max readiness" {
				abort, undo = true, i
			} else {
				fmt.Println("failed to update peer's readiness: ", err)
			}
		}
	}

	// if aborted, revert notified peers
	for _, client := range toNotify[0:undo] {
		if _, err := client.UpdateReadiness(router.ctx, &peer.ReadinessRequest{
			Ordinal: router.ordinal,
			Readiness: []*peer.Readiness{{
				ResourceType:  uint32(resourceType),
				Readiness:     []byte(resource.readiness),
				ReadyForEqual: resource.readyForEqual,
				Max:           resource.readinessMax,
			}},
			ShuttingDown: shuttingDown,
		}); err != nil {
			fmt.Println("failed to revert peer's readiness: ", err)
		}
	}

	if (!increase && !abort) || resource.decreasingFromMaxReadiness {

		router.peerMutex.Lock()
		if !increase && !abort {
			// change which requests we will accept locally
			resource.readiness, resource.readyForEqual, resource.readinessMax = readiness, readyForEqual, readinessMax
		}
		if resource.decreasingFromMaxReadiness {
			resource.decreasingFromMaxReadiness = false
		}
		router.peerMutex.Unlock()
	}
}
