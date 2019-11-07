package router

import "github.com/kent-h/stateful-router/protos/peer"

type rebalanceEventData struct {
	ch            chan struct{}
	newPeers      map[peer.PeerClient]struct{}
	resourceTypes map[ResourceType]struct{}
}

type deviceCountEventData struct {
	ch             chan struct{}
	updateComplete chan struct{}

	resources map[ResourceType]map[uint32]uint32
}

func (router *Router) rebalanceAll() {
	router.eventMutex.Lock()
	defer router.eventMutex.Unlock()

	router.rebalanceAllUnsafe()
}

func (router *Router) rebalance(resourceType ResourceType) {
	router.eventMutex.Lock()
	defer router.eventMutex.Unlock()

	router.rebalanceUnsafe(resourceType)
}

func (router *Router) deviceCountChanged(resourceType ResourceType) {
	router.eventMutex.Lock()
	defer router.eventMutex.Unlock()

	router.rebalanceUnsafe(resourceType)

	router.deviceCountChangedUnsafe(resourceType)
}

func (router *Router) peerConnected(node *node) {
	router.eventMutex.Lock()
	defer router.eventMutex.Unlock()

	router.rebalanceEventData.newPeers[node] = struct{}{}
	router.rebalanceAllUnsafe()

	router.deviceCountAllChangedUnsafe()
}

// used in order to proxy an update from a given peer
// during migration, uninvolved peers are be notified of both peer's device count changes in a single update
// this way, uninvolved peers won't see a change in the total device count
func (router *Router) deviceCountChangedWithUpdateForPeer(resourceType ResourceType, nodeId, resourceCount uint32) chan struct{} {
	router.eventMutex.Lock()
	defer router.eventMutex.Unlock()

	router.rebalanceUnsafe(resourceType)

	router.deviceCountChangedUnsafe(resourceType)
	router.deviceCountEventData.resources[resourceType][nodeId] = resourceCount

	return router.deviceCountEventData.updateComplete
}

func (router *Router) deviceCountChangedUnsafe(resourceType ResourceType) {
	if router.deviceCountEventData.ch != nil {
		close(router.deviceCountEventData.ch)
		router.deviceCountEventData.ch = nil
	}

	if _, have := router.deviceCountEventData.resources[resourceType]; !have {
		router.deviceCountEventData.resources[resourceType] = make(map[uint32]uint32)
	}
}

func (router *Router) deviceCountAllChangedUnsafe() {
	if router.deviceCountEventData.ch != nil {
		close(router.deviceCountEventData.ch)
		router.deviceCountEventData.ch = nil
	}

	for resourceType := range router.resources {
		if _, have := router.deviceCountEventData.resources[resourceType]; !have {
			router.deviceCountEventData.resources[resourceType] = make(map[uint32]uint32)
		}
	}
}

func (router *Router) rebalanceUnsafe(resourceType ResourceType) {
	if router.rebalanceEventData.ch != nil {
		close(router.rebalanceEventData.ch)
		router.rebalanceEventData.ch = nil
	}
	router.rebalanceEventData.resourceTypes[resourceType] = struct{}{}
}

func (router *Router) rebalanceAllUnsafe() {
	if router.rebalanceEventData.ch != nil {
		close(router.rebalanceEventData.ch)
		router.rebalanceEventData.ch = nil
	}

	for resourceType := range router.resources {
		router.rebalanceEventData.resourceTypes[resourceType] = struct{}{}
	}
}
