package router

import (
	"context"
	"errors"
	"fmt"
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
	router.peerMutex.Lock()
	defer router.peerMutex.Unlock()

	for resourceType, resourceStat := range request.ResourceStats {
		resourceType := ResourceType(resourceType)
		for _, nodeStat := range resourceStat.Stats {
			if node, have := router.peers[nodeStat.Ordinal]; have {
				resourceData := node.resources[resourceType]
				resourceData.count = nodeStat.Count
				node.resources[resourceType] = resourceData
			}
		}
		router.rebalance(resourceType)
	}
	return &empty.Empty{}, nil
}

func (router peerApi) NextResource(ctx context.Context, request *peer.NextResourceRequest) (*peer.NextResourceResponse, error) {
	resource, have := router.resources[ResourceType(request.ResourceType)]
	if !have {
		return &peer.NextResourceResponse{}, fmt.Errorf(errorResourceTypeNotFound, ResourceType(request.ResourceType))
	}
	resource.deviceMutex.RLock()
	defer resource.deviceMutex.RUnlock()

	migrateNext := ""
	found := false
	first, isOnlyDeviceToMigrate := true, false
	if !request.ReadinessMax {
		for deviceId := range resource.devices {
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
	return &peer.NextResourceResponse{Has: found, ResourceId: []byte(migrateNext), Last: isOnlyDeviceToMigrate}, nil
}

func (router peerApi) UpdateReadiness(ctx context.Context, request *peer.ReadinessRequest) (*empty.Empty, error) {
	router.peerMutex.Lock()
	node := router.peers[request.Ordinal]
	shuttingDownStateChanged := node.shuttingDown != request.ShuttingDown
	// no matter what, set the shutting-down state
	node.shuttingDown = request.ShuttingDown

	// for each resource, verify that there will be at least one node with max readiness
	for _, readiness := range request.Readiness {
		resourceType := ResourceType(readiness.ResourceType)
		node := node.resources[resourceType]
		routerResource := router.resources[resourceType]

		// if decreasing from max readiness
		if node.readinessMax && !readiness.Max {
			// complain if no other node exists with max readiness
			if !routerResource.readinessMax || routerResource.decreasingFromMaxReadiness {
				foundMax := false
				for nodeId, otherNode := range router.peers {
					if nodeId != request.Ordinal && otherNode.resources[resourceType].readinessMax {
						foundMax = true
						break
					}
				}
				if !foundMax {
					router.peerMutex.Unlock()

					if shuttingDownStateChanged {
						router.rebalanceAll()
					}
					return &empty.Empty{}, errors.New("no other nodes exist with max readiness")
				}
			}
		}
	}

	// -- if this line is reached, we will accept the readiness change --

	recalculateReadiness := make(map[ResourceType]struct{})

	for _, requestReadiness := range request.Readiness {
		resourceType := ResourceType(requestReadiness.ResourceType)
		nodeResource := node.resources[resourceType]
		if shuttingDownStateChanged || nodeResource.readiness != string(requestReadiness.Readiness) || nodeResource.readyForEqual != requestReadiness.ReadyForEqual || nodeResource.readinessMax != requestReadiness.Max {
			recalculateReadiness[resourceType] = struct{}{}
		}
		nodeResource.readiness, nodeResource.readyForEqual, nodeResource.readinessMax = string(requestReadiness.Readiness), requestReadiness.ReadyForEqual, requestReadiness.Max
		node.resources[resourceType] = nodeResource
	}
	router.peerMutex.Unlock()

	for _, requestResource := range request.Readiness {
		requestResourceType := ResourceType(requestResource.ResourceType)
		routerResource := router.resources[requestResourceType]

		routerResource.deviceMutex.Lock()
		originalDeviceCount := uint32(len(routerResource.devices))
		devicesToMove := make(map[string]*deviceData)
		for deviceId, device := range routerResource.devices {
			//for every device that belongs on the other node
			if BestNode(deviceId, router.ordinal, map[uint32]struct{}{request.Ordinal: {}}) == request.Ordinal {
				// if the other node is ready for this device
				if deviceId < string(requestResource.Readiness) || (deviceId == string(requestResource.Readiness) && requestResource.ReadyForEqual) {
					//release and notify that it's moved
					devicesToMove[deviceId] = device
					delete(routerResource.devices, deviceId)
				}
			}
		}
		routerResource.deviceMutex.Unlock()

		router.migrateResources(requestResourceType, devicesToMove, originalDeviceCount)

		if shuttingDownStateChanged {
			router.rebalanceAll()
		} else {
			for resourceType := range recalculateReadiness {
				router.rebalance(resourceType)
			}
		}
	}

	return &empty.Empty{}, nil
}

// Handoff is just basically hint to load a device, so we'll do normal loading/locking
func (router peerApi) Handoff(ctx context.Context, request *peer.HandoffRequest) (*empty.Empty, error) {
	var peerUpdateComplete chan struct{}
	if mutex, remoteHandler, forward, err := router.locate(ResourceType(request.ResourceType), string(request.ResourceId), func(resourceType ResourceType) {
		peerUpdateComplete = router.deviceCountChangedWithUpdateForPeer(resourceType, request.Ordinal, request.ResourceCount)
	}, router.loader.Load); err != nil {
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
