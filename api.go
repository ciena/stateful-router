package router

import (
	"context"
	"github.com/kent-h/stateful-router/protos/peer"
	"google.golang.org/grpc"
	"time"
)

const errorResourceTypeNotFound = "the resource type %d does not exist"

type Loader interface {
	Load(ctx context.Context, resourceType ResourceType, resourceId string) error
	Unload(resourceType ResourceType, resourceId string)
}

func New(server *grpc.Server, ordinal uint32, peerDNSFormat string, loader Loader, readyCallback func(), resourceTypes ...ResourceType) *Router {
	ctx, ctxCancelFunc := context.WithCancel(context.Background())
	handoffAndShutdown := make(chan struct{})
	router := &Router{
		ordinal:            ordinal,
		peerDNSFormat:      peerDNSFormat,
		handoffAndShutdown: handoffAndShutdown,
		ctx:                ctx,
		peers:              make(map[uint32]*node),
		resources:          make(map[ResourceType]*resourceData),
		resourceCountEventData: resourceCountEventData{
			updateComplete: make(chan struct{}),
			resources:      make(map[ResourceType]map[uint32]uint32),
		},
		rebalanceEventData: rebalanceEventData{
			newPeers:      make(map[peer.PeerClient]struct{}),
			resourceTypes: make(map[ResourceType]struct{}),
		},
		statusNotifierDone: make(chan struct{}),
		rebalancerDone:     make(chan struct{}),
		loader:             loader,
	}

	// create data structure for each resource type
	if len(resourceTypes) == 0 {
		panic("at least one resources type must be provided for routing")
	}
	for _, resourceType := range resourceTypes {
		router.resources[resourceType] = &resourceData{
			loaded: make(map[string]*syncher),
		}
	}

	// connect to all nodes with smaller ordinal than ours
	for i := uint32(0); i < router.ordinal; i++ {
		router.connect(i)
	}

	go router.startStatsNotifier()

	// node will only start accepting requests after rebalancer is started (default readiness allows no requests)
	time.AfterFunc(waitReadyTime, func() {
		if readyCallback != nil {
			readyCallback()
		}
		router.startRebalancer(router.handoffAndShutdown, ctxCancelFunc)
	})

	peer.RegisterPeerServer(server, peerApi{router})
	return router
}

func (router *Router) Stop() {
	router.eventMutex.Lock()
	if router.handoffAndShutdown != nil {
		close(router.handoffAndShutdown)
		router.handoffAndShutdown = nil
	}
	router.eventMutex.Unlock()

	<-router.ctx.Done()
	<-router.statusNotifierDone
}

// this should be called when a device has been, or should be,
// unloaded **from the local node only** due to external circumstance
// (device lock lost, device deleted, inactivity timeout, etc.)
func (router *Router) UnloadDevice(resourceType ResourceType, key string) {
	router.UnloadDeviceWithUnloadFunc(resourceType, key, router.loader.Unload)
}

func (router *Router) UnloadDeviceWithUnloadFunc(resourceType ResourceType, resourceId string, unloadFunc func(resourceType ResourceType, resourceId string)) {
	resource := router.resources[resourceType]

	resource.mutex.Lock()
	sync, have := resource.loaded[resourceId]
	if have {
		delete(resource.loaded, resourceId)
	}
	resource.mutex.Unlock()

	if have {
		router.resourceCountChanged(resourceType)
		router.unloadResource(resourceType, resourceId, sync, unloadFunc)
	}
}

// Locate returns a processor for the given device,
// to either handle the request locally,
// or forward it on to the appropriate peer
func (router *Router) Locate(resourceType ResourceType, resourceId string) (interface{ RUnlock() }, *grpc.ClientConn, bool, error) {
	return router.locate(resourceType, resourceId, router.resourceCountChanged, router.loader.Load)
}

func (router *Router) LocateWithLoadFunc(resourceType ResourceType, resourceId string, loadFunc func(ctx context.Context, resourceType ResourceType, deviceId string) error) (interface{ RUnlock() }, *grpc.ClientConn, bool, error) {
	return router.locate(resourceType, resourceId, router.resourceCountChanged, loadFunc)
}
