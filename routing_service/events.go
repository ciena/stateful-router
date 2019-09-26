package routing

import (
	"context"
	"fmt"
	"github.com/khagerma/stateful-experiment/protos/peer"
	"math"
)

type eventData struct {
	ch                 chan struct{}
	newPeers           map[*node]struct{}
	deviceCountChanged bool
}

func (router *routingService) deviceCountChanged() {
	router.eventMutex.Lock()
	defer router.eventMutex.Unlock()

	router.eventData.deviceCountChanged = true
	if router.eventData.ch != nil {
		close(router.eventData.ch)
		router.eventData.ch = nil
	}
}

func (router *routingService) peerConnected(node *node) {
	router.eventMutex.Lock()
	defer router.eventMutex.Unlock()

	router.eventData.newPeers[node] = struct{}{}
	if router.eventData.ch != nil {
		close(router.eventData.ch)
		router.eventData.ch = nil
	}
}

func (router *routingService) recalculateReadiness() {
	router.eventMutex.Lock()
	defer router.eventMutex.Unlock()

	if router.eventData.ch != nil {
		close(router.eventData.ch)
		router.eventData.ch = nil
	}
}

func (router *routingService) startEventHandler() {
	for {
		router.eventMutex.Lock()
		data := router.eventData
		ch := make(chan struct{})
		router.eventData = eventData{
			ch:                 ch,
			newPeers:           make(map[*node]struct{}),
			deviceCountChanged: false,
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

		// inform peers of changes to device count
		if data.deviceCountChanged {
			router.peerMutex.RLock()
			toUpdate := make(map[*node]struct{}, len(router.peers))
			for _, node := range router.peers {
				if node.connected {
					toUpdate[node] = struct{}{}
				}
			}
			router.peerMutex.RUnlock()

			for node := range toUpdate {
				if _, err := node.UpdateStats(context.Background(), &peer.StatsRequest{Ordinal: router.ordinal, Devices: deviceCount}); err != nil {
					fmt.Println(err)
				}
			}
		}

		// repeat flag for readiness change
		done := false

		// recalculate readiness
		if !router.ready {
			done = true
		} else {
			// determine next higher & lower devices
			proposedIncrementReadiness, have, isLast := router.searchPeersForNextDevice(router.readiness)

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
			var incrementNodes uint32
			var decrementNodes uint32
			var incrementLargerNodes uint64
			var decrementLargerNodes uint64
			var devicesOnIncrementNodes uint64
			var devicesOnDecrementNodes uint64

			router.peerMutex.RLock()
			for nodeId, node := range router.peers {
				if node.connected {
					if node.readiness > router.readiness {
						shouldJumpToMax = false
					}
					// only take into account nodes with similar number of, or more, devices
					if node.devices >= deviceCount+1 || node.readiness == math.MaxUint64 {
						devicesOnIncrementNodes += uint64(node.devices)
						incrementNodes++
						if nodeId > router.ordinal {
							incrementLargerNodes++
						}
					}
					if node.devices+1 >= deviceCount || node.readiness == math.MaxUint64 {
						devicesOnDecrementNodes += uint64(node.devices)
						decrementNodes++
						if nodeId > router.ordinal {
							decrementLargerNodes++
						}
					}
				}
			}
			router.peerMutex.RUnlock()

			var incrementDevicesMoved uint64
			if have {
				incrementDevicesMoved = 1
				isLast = isLast
			}

			incrementIsInvalid, decrementIsInvalid := false, false

			var averageDevicesPerNode uint32
			var spareDevices uint32
			if devicesOnDecrementNodes != 0 {
				averageDevicesPerNode = uint32(devicesOnDecrementNodes / uint64(decrementNodes))
				spareDevices = uint32(devicesOnDecrementNodes % uint64(decrementNodes))
			}

			if deviceCount < averageDevicesPerNode || (deviceCount == averageDevicesPerNode && spareDevices > (decrementNodes/2)) {
				// pull device
				incrementIsInvalid = true
			}

			if deviceCount > averageDevicesPerNode+1 {
				// push device
				decrementIsInvalid = true
			}

			incrementLeft := averageDevicesPerNode
			incrementRight := uint64(deviceCount) + 1

			decrementLeft := averageDevicesPerNode
			decrementRight := uint64(deviceCount)

			//incrementIsInvalid = incrementLeft > incrementRight
			//devicesOnIncrementNodes+incrementNodes >= (uint64(deviceCount)+incrementDevicesMoved)*incrementNodes+incrementDevicesMoved
			//decrementIsInvalid = decrementLeft < decrementRight

			fmt.Println(deviceCount, "devices currently", incrementDevicesMoved)
			fmt.Println(devicesOnIncrementNodes, "devices across", incrementNodes, "nodes,", incrementLargerNodes, "of which have a larger ordinal", incrementLeft, ">", incrementRight, "=", incrementIsInvalid)
			fmt.Println(devicesOnDecrementNodes, "devices across", decrementNodes, "nodes,", decrementLargerNodes, "of which have a larger ordinal", decrementLeft, "<", decrementRight, "=", decrementIsInvalid)

			/*if shouldJumpToMax {
				if router.readiness != math.MaxUint64 {
					fmt.Println("Jumping to max: Changing readiness to", uint64(math.MaxUint64))
					router.changeReadinessTo(math.MaxUint64)
				}
				done = true
			} else*/
			if router.readiness != math.MaxUint64 && (shouldJumpToMax || incrementIsInvalid) {
				fmt.Println("Increment: Changing readiness to", proposedIncrementReadiness)
				router.changeReadinessTo(proposedIncrementReadiness)
			} else if router.readiness != 0 && decrementIsInvalid {
				fmt.Println("Decrement: Changing readiness to", proposedDecrementReadiness)
				router.changeReadinessTo(proposedDecrementReadiness)
			} else {
				// we're done, and don't need to repeat unless something changes
				done = true
			}

			// if readiness has decreased, kick out any devices that no longer belong on this node
			router.deviceMutex.Lock()
			devicesToMove := make(map[uint64]*deviceData)
			for deviceId, device := range router.devices {
				//for every device that no longer belongs on this node
				if deviceId > router.readiness {
					fmt.Println("will migrate device", deviceId)
					//release and notify that it's moved
					devicesToMove[deviceId] = device
					delete(router.devices, deviceId)
					router.deviceCountChanged()
				}
			}
			router.deviceMutex.Unlock()

			// migrate devices
			for deviceId, device := range devicesToMove {
				router.migrateDevice(deviceId, device)
			}
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
