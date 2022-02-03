package router

import "github.com/kent-h/stateful-router/protos/peer"

type rebalanceEventData struct {
	ch            chan struct{}
	newPeers      map[peer.PeerClient]struct{}
	resourceTypes map[ResourceType]struct{}
}

type resourceCountEventData struct {
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

func (router *Router) resourceCountChanged(resourceType ResourceType) {
	router.eventMutex.Lock()
	defer router.eventMutex.Unlock()

	router.rebalanceUnsafe(resourceType)

	router.resourceCountChangedUnsafe(resourceType)
}

func (router *Router) peerConnected(node *node) {
	router.eventMutex.Lock()
	defer router.eventMutex.Unlock()

	router.rebalanceEventData.newPeers[node] = struct{}{}
	router.rebalanceAllUnsafe()

	router.resourceCountAllChangedUnsafe()
}

// used in order to proxy an update from a given peer
// during migration, uninvolved peers are be notified of both peer's resource count changes in a single update
// this way, uninvolved peers won't see a change in the total resource count
func (router *Router) resourceCountChangedWithUpdateForPeer(resourceType ResourceType, nodeId, resourceCount uint32) chan struct{} {
	router.eventMutex.Lock()
	defer router.eventMutex.Unlock()

	router.rebalanceUnsafe(resourceType)

	router.resourceCountChangedUnsafe(resourceType)
	router.resourceCountEventData.resources[resourceType][nodeId] = resourceCount

	return router.resourceCountEventData.updateComplete
}

func (router *Router) resourceCountChangedUnsafe(resourceType ResourceType) {
	if router.resourceCountEventData.ch != nil {
		close(router.resourceCountEventData.ch)
		router.resourceCountEventData.ch = nil
	}

	if _, have := router.resourceCountEventData.resources[resourceType]; !have {
		router.resourceCountEventData.resources[resourceType] = make(map[uint32]uint32)
	}
}

func (router *Router) resourceCountAllChangedUnsafe() {
	if router.resourceCountEventData.ch != nil {
		close(router.resourceCountEventData.ch)
		router.resourceCountEventData.ch = nil
	}

	for resourceType := range router.resources {
		if _, have := router.resourceCountEventData.resources[resourceType]; !have {
			router.resourceCountEventData.resources[resourceType] = make(map[uint32]uint32)
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
