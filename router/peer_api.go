package routing

import (
	"context"
	"github.com/golang/protobuf/ptypes/empty"
	"github.com/khagerma/stateful-experiment/protos/peer"
	"github.com/pkg/errors"
	"math"
)

func (router *router) Hello(ctx context.Context, request *peer.HelloRequest) (*empty.Empty, error) {
	router.connect(request.Ordinal)
	return &empty.Empty{}, nil
}

func (router *router) UpdateStats(ctx context.Context, request *peer.StatsRequest) (*empty.Empty, error) {
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

func (router *router) NextDevice(ctx context.Context, request *peer.NextDeviceRequest) (*peer.NextDeviceResponse, error) {
	router.deviceMutex.RLock()
	defer router.deviceMutex.RUnlock()

	migrateNext := uint64(math.MaxUint64)
	found := false
	first, isOnlyDeviceToMigrate := true, false
	for deviceId := range router.devices {
		// for every device that belongs on the other node
		if BestNode(deviceId, router.ordinal, map[uint32]struct{}{request.Ordinal: {}}) == request.Ordinal {
			// find the lowest device that's > other.readiness
			if deviceId >= request.Readiness && !request.MaxReadiness {
				found = true
				if first {
					first = false
					isOnlyDeviceToMigrate = true
				} else {
					isOnlyDeviceToMigrate = false
				}
				if deviceId < migrateNext {
					migrateNext = deviceId
				}
			}
		}
	}
	return &peer.NextDeviceResponse{Has: found, Device: migrateNext, Last: isOnlyDeviceToMigrate}, nil
}

func (router *router) UpdateReadiness(ctx context.Context, request *peer.ReadinessRequest) (*empty.Empty, error) {
	router.peerMutex.Lock()
	recalculateReadiness := false
	node := router.peers[request.Ordinal]
	// if decreasing from max readiness
	if node.maxReadiness && !request.MaxReadiness {
		// complain if no other node exists with max readiness
		if !router.maxReadiness || router.decreasingFromMaxReadiness {
			foundMax := false
			for nodeId, node := range router.peers {
				if nodeId != request.Ordinal && node.maxReadiness {
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

	recalculateReadiness = node.readiness != request.Readiness || node.maxReadiness != request.MaxReadiness
	node.readiness, node.maxReadiness = request.Readiness, request.MaxReadiness
	router.peerMutex.Unlock()

	router.deviceMutex.Lock()
	originalDeviceCount := uint32(len(router.devices))
	devicesToMove := make(map[uint64]*deviceData)
	for deviceId, device := range router.devices {
		//for every device that belongs on the other node
		if BestNode(deviceId, router.ordinal, map[uint32]struct{}{request.Ordinal: {}}) == request.Ordinal {
			// if the other node is ready for this device
			if deviceId < request.Readiness {
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
func (router *router) Handoff(ctx context.Context, request *peer.HandoffRequest) (*empty.Empty, error) {
	var peerUpdateComplete chan struct{}
	if mutex, remoteHandler, forward, err := router.handlerFor(request.Device, func() { peerUpdateComplete = router.deviceCountPeerChanged(request.Ordinal, request.Devices) }); err != nil {
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
