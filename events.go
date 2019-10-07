package router

import "github.com/kent-h/stateful-router/protos/peer"

type rebalanceEventData struct {
	ch       chan struct{}
	newPeers map[peer.PeerClient]struct{}
}

type deviceCountEventData struct {
	ch             chan struct{}
	updateComplete chan struct{}
	updatingPeers  map[uint32]uint32
}

func (router *Router) rebalance() {
	router.eventMutex.Lock()
	defer router.eventMutex.Unlock()

	router.rebalanceUnsafe()
}

func (router *Router) deviceCountChanged() {
	router.eventMutex.Lock()
	defer router.eventMutex.Unlock()

	router.rebalanceUnsafe()

	router.deviceCountChangedUnsafe()
}

func (router *Router) peerConnected(node *node) {
	router.eventMutex.Lock()
	defer router.eventMutex.Unlock()

	router.rebalanceEventData.newPeers[node] = struct{}{}
	router.rebalanceUnsafe()

	router.deviceCountChangedUnsafe()
}

func (router *Router) deviceCountPeerChanged(nodeId uint32, devices uint32) chan struct{} {
	router.eventMutex.Lock()
	defer router.eventMutex.Unlock()

	router.rebalanceUnsafe()

	router.deviceCountEventData.updatingPeers[nodeId] = devices
	router.deviceCountChangedUnsafe()

	return router.deviceCountEventData.updateComplete
}

func (router *Router) deviceCountChangedUnsafe() {
	if router.deviceCountEventData.ch != nil {
		close(router.deviceCountEventData.ch)
		router.deviceCountEventData.ch = nil
	}
}

func (router *Router) rebalanceUnsafe() {
	if router.rebalanceEventData.ch != nil {
		close(router.rebalanceEventData.ch)
		router.rebalanceEventData.ch = nil
	}
}
