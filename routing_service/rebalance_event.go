package routing

import (
	"context"
	"fmt"
	"github.com/khagerma/stateful-experiment/protos/peer"
	"math"
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
			if _, err := node.Hello(context.Background(), &peer.HelloRequest{Ordinal: router.ordinal}); err != nil {
				fmt.Println(err)
			}
			if router.ready {
				if _, err := node.UpdateReadiness(context.Background(), &peer.ReadinessRequest{Ordinal: router.ordinal, Readiness: router.readiness}); err != nil {
					fmt.Println(err)
				}
			}
		}

		router.deviceMutex.RLock()
		deviceCount := uint32(len(router.devices))
		router.deviceMutex.RUnlock()

		// repeat flag for readiness change
		done := false

		// recalculate readiness
		if !router.ready {
			done = true
		} else {
			// determine next higher & lower devices
			proposedIncrementReadiness, _, _ := router.searchPeersForNextDevice(router.readiness)

			router.deviceMutex.RLock()
			var proposedDecrementReadiness uint64
			for deviceId := range router.devices {
				if deviceId > proposedDecrementReadiness && deviceId > 0 {
					proposedDecrementReadiness = deviceId - 1
				}
			}
			router.deviceMutex.RUnlock()

			// we want to jump to max (accept all devices) if all nodes w/ > readiness are suddenly missing
			// we want to increment if devices < average # of devices on nodes w/ >= the proposed readiness
			// we want to decrement if devices > average # of devices on nodes w/ > the proposed readiness
			shouldJumpToMax := router.readiness != math.MaxUint64
			var numPeers uint64
			var numGreaterOrdinalNodes uint64
			var peersDeviceCount uint64

			router.peerMutex.RLock()
			for nodeId, node := range router.peers {
				if node.connected {
					if node.readiness > router.readiness {
						shouldJumpToMax = false
					}
					// only take into account nodes with similar number of, or more, devices
					if node.devices+2 >= deviceCount || node.readiness == math.MaxUint64 {
						peersDeviceCount += uint64(node.devices)
						numPeers++
						if nodeId > router.ordinal {
							numGreaterOrdinalNodes++
						}
					}
				}
			}

			sortedPeers := make([]uint32, len(router.peers))
			ctr := 0
			for nodeId := range router.peers {
				sortedPeers[ctr] = nodeId
				ctr++
			}
			sort.Slice(sortedPeers, func(i, j int) bool { return sortedPeers[i] < sortedPeers[j] })

			increment, decrement := false, false
			if numPeers != 0 {
				// calculate how many devices each peer should have
				peersShouldHave := make(map[uint32]uint32, len(sortedPeers))
				for i, nodeId := range sortedPeers {
					peersShouldHave[nodeId] = uint32(peersDeviceCount / numPeers)
					// if to the left of the shouldHave line
					if uint64(i) < peersDeviceCount%numPeers {
						peersShouldHave[nodeId]++
					}
				}
				fmt.Println("peersShouldHave", peersShouldHave)

				maxDevices := uint32((peersDeviceCount + numGreaterOrdinalNodes) / numPeers)
				minDevices := int64(maxDevices)
				if (peersDeviceCount+numGreaterOrdinalNodes)%numPeers == 0 {
					minDevices-- // ?
				}
				fmt.Println("Have", deviceCount, "should have between", minDevices, "and", maxDevices)

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
						if !(node.devices >= peersShouldHave[nodeId] || node.readiness == math.MaxUint64) {
							allPeersMeetRequisite = false
							break
						}
					}
					if allPeersMeetRequisite {
						// push
						decrement = true
					}
				}
			}
			router.peerMutex.RUnlock()

			if router.readiness != math.MaxUint64 && (shouldJumpToMax || increment) {
				fmt.Println("Increment: Changing readiness to", proposedIncrementReadiness)
				router.changeReadinessTo(proposedIncrementReadiness)
			} else if router.readiness != 0 && decrement {
				fmt.Println("Decrement: Changing readiness to", proposedDecrementReadiness)
				router.changeReadinessTo(proposedDecrementReadiness)
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
				if deviceId > router.readiness {
					fmt.Println("will migrate device", deviceId)
					//release and notify that it's moved
					devicesToMove[deviceId] = device
					delete(router.devices, deviceId)
				}
			}
			router.deviceMutex.Unlock()

			router.migrateDevices(devicesToMove, originalDeviceCount)
		}

		if done {
			// wait until something changes
			select {
			case <-ch:
				// event received
			}
		}
	}
}
