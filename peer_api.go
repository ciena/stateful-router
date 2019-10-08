package router

import (
	"context"
	"errors"
	"github.com/golang/protobuf/ptypes/empty"
	"github.com/kent-h/stateful-router/protos/peer"
)

type peerApi struct {
	*Router
}

func (router peerApi) Hello(ctx context.Context, request *peer.HelloRequest) (*empty.Empty, error) {
	router.connect(request.Ordinal)
	return &empty.Empty{}, nil
}

func (router peerApi) UpdateStats(ctx context.Context, request *peer.StatsRequest) (*empty.Empty, error) {
	router.peerMutex.RLock()
	defer router.peerMutex.RUnlock()

	for ordinal, devices := range request.DeviceCounts {
		if node, have := router.peers[ordinal]; have {
			node.devices = devices
		}
	}
	router.rebalance()
	return &empty.Empty{}, nil
}

func (router peerApi) NextDevice(ctx context.Context, request *peer.NextDeviceRequest) (*peer.NextDeviceResponse, error) {
	router.deviceMutex.RLock()
	defer router.deviceMutex.RUnlock()

	migrateNext := ""
	found := false
	first, isOnlyDeviceToMigrate := true, false
	if !request.ReadinessMax {
		for deviceId := range router.devices {
			// for every device that belongs on the other node
			if BestNode(deviceId, router.ordinal, map[uint32]struct{}{request.Ordinal: {}}) == request.Ordinal {
				// find the lowest device that's > other.readiness
				if deviceId > string(request.Readiness) || (deviceId == string(request.Readiness) && !request.ReadyForEqual) {
					found = true
					if first {
						first = false
						migrateNext = deviceId
						isOnlyDeviceToMigrate = true
					} else {
						isOnlyDeviceToMigrate = false
						if deviceId < migrateNext {
							migrateNext = deviceId
						}
					}
				}
			}
		}
	}
	return &peer.NextDeviceResponse{Has: found, Device: []byte(migrateNext), Last: isOnlyDeviceToMigrate}, nil
}

func (router peerApi) UpdateReadiness(ctx context.Context, request *peer.ReadinessRequest) (*empty.Empty, error) {
	router.peerMutex.Lock()
	recalculateReadiness := false
	node := router.peers[request.Ordinal]
	// if decreasing from max readiness
	if node.readinessMax && !request.ReadinessMax {
		// complain if no other node exists with max readiness
		if !router.readinessMax || router.decreasingFromMaxReadiness {
			foundMax := false
			for nodeId, node := range router.peers {
				if nodeId != request.Ordinal && node.readinessMax {
					foundMax = true
					break
				}
			}
			if !foundMax {
				router.peerMutex.Unlock()
				return &empty.Empty{}, errors.New("no other nodes exist with max readiness")
			}
		}
	}

	recalculateReadiness = node.readiness != string(request.Readiness) || node.readyForEqual != request.ReadyForEqual || node.readinessMax != request.ReadinessMax
	node.readiness, node.readyForEqual, node.readinessMax = string(request.Readiness), request.ReadyForEqual, request.ReadinessMax
	router.peerMutex.Unlock()

	router.deviceMutex.Lock()
	originalDeviceCount := uint32(len(router.devices))
	devicesToMove := make(map[string]*deviceData)
	for deviceId, device := range router.devices {
		//for every device that belongs on the other node
		if BestNode(deviceId, router.ordinal, map[uint32]struct{}{request.Ordinal: {}}) == request.Ordinal {
			// if the other node is ready for this device
			if deviceId < string(request.Readiness) || (deviceId == string(request.Readiness) && request.ReadyForEqual) {
				//release and notify that it's moved
				devicesToMove[deviceId] = device
				delete(router.devices, deviceId)
			}
		}
	}
	router.deviceMutex.Unlock()

	router.migrateDevices(devicesToMove, originalDeviceCount)

	if recalculateReadiness {
		router.rebalance()
	}

	return &empty.Empty{}, nil
}

// Handoff is just basically hint to load a device, so we'll do normal loading/locking
func (router peerApi) Handoff(ctx context.Context, request *peer.HandoffRequest) (*empty.Empty, error) {
	var peerUpdateComplete chan struct{}
	if mutex, remoteHandler, forward, err := router.locate(string(request.Device), func() { peerUpdateComplete = router.deviceCountPeerChanged(request.Ordinal, request.Devices) }); err != nil {
		return &empty.Empty{}, err
	} else if forward {
		return peer.NewPeerClient(remoteHandler).Handoff(ctx, request)
	} else {
		mutex.RUnlock()
		if peerUpdateComplete != nil {
			select {
			case <-peerUpdateComplete:
			case <-ctx.Done():
			}
		}
		return &empty.Empty{}, nil
	}
}
