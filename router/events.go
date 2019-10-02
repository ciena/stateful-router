package routing

import "github.com/khagerma/stateful-experiment/protos/peer"

type rebalanceEventData struct {
	ch       chan struct{}
	newPeers map[peer.PeerClient]struct{}
}

type deviceCountEventData struct {
	ch             chan struct{}
	updateComplete chan struct{}
	updatingPeers  map[uint32]uint32
}

func (router *router) rebalance() {
	router.eventMutex.Lock()
	defer router.eventMutex.Unlock()

	router.rebalanceUnsafe()
}

func (router *router) deviceCountChanged() {
	router.eventMutex.Lock()
	defer router.eventMutex.Unlock()

	router.rebalanceUnsafe()

	router.deviceCountChangedUnsafe()
}

func (router *router) peerConnected(node *node) {
	router.eventMutex.Lock()
	defer router.eventMutex.Unlock()

	router.rebalanceEventData.newPeers[node] = struct{}{}
	router.rebalanceUnsafe()

	router.deviceCountChangedUnsafe()
}

func (router *router) deviceCountPeerChanged(nodeId uint32, devices uint32) chan struct{} {
	router.eventMutex.Lock()
	defer router.eventMutex.Unlock()

	router.rebalanceUnsafe()

	router.deviceCountEventData.updatingPeers[nodeId] = devices
	router.deviceCountChangedUnsafe()

	return router.deviceCountEventData.updateComplete
}

func (router *router) deviceCountChangedUnsafe() {
	if router.deviceCountEventData.ch != nil {
		close(router.deviceCountEventData.ch)
		router.deviceCountEventData.ch = nil
	}
}

func (router *router) rebalanceUnsafe() {
	if router.rebalanceEventData.ch != nil {
		close(router.rebalanceEventData.ch)
		router.rebalanceEventData.ch = nil
	}
}
