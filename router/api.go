package routing

import (
	"context"
	"github.com/khagerma/stateful-experiment/protos/peer"
	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"
	"time"
)

func New(ordinal uint32, peerDNSFormat string, loader DeviceLoader) (Router, *grpc.Server) {
	ctx, ctxCancelFunc := context.WithCancel(context.Background())
	router := &router{
		ordinal:       ordinal,
		peerDNSFormat: peerDNSFormat,
		ctx:           ctx,
		ctxCancelFunc: ctxCancelFunc,
		peers:         make(map[uint32]*node),
		devices:       make(map[uint64]*deviceData),
		deviceCountEventData: deviceCountEventData{
			updateComplete: make(chan struct{}),
			updatingPeers:  make(map[uint32]uint32),
		},
		rebalanceEventData: rebalanceEventData{
			newPeers: make(map[peer.PeerClient]struct{}),
		},
		deviceCountEventHandlerDone: make(chan struct{}),
		rebalanceEventHandlerDone:   make(chan struct{}),
		loader:                      loader,
	}

	for i := uint32(0); i < router.ordinal; i++ {
		router.connect(i)
	}

	// node will only start accepting requests after rebalancer is started (default readiness allows no requests)
	time.AfterFunc(waitReadyTime, router.startRebalancer)
	go router.startStatsNotifier()

	// new grpc server
	server := grpc.NewServer(grpc.KeepaliveParams(
		keepalive.ServerParameters{
			Time:    keepaliveTime,
			Timeout: keepaliveTimeout,
		}), grpc.KeepaliveEnforcementPolicy(keepalive.EnforcementPolicy{
		MinTime:             keepaliveTime / 2,
		PermitWithoutStream: true,
	}))

	peer.RegisterPeerServer(server, router)
	return Router{router}, server
}

func (r Router) Stop() {
	r.r.ctxCancelFunc()
	<-r.r.rebalanceEventHandlerDone
	<-r.r.deviceCountEventHandlerDone
}

// this should be called when a device has been, or should be,
// unloaded **from the local node only** due to external circumstance
// (device lock lost, device deleted, inactivity timeout, etc.)
func (r Router) UnloadDevice(deviceId uint64) {
	r.r.deviceMutex.Lock()
	device, have := r.r.devices[deviceId]
	if have {
		delete(r.r.devices, deviceId)
		r.r.deviceCountChanged()
	}
	r.r.deviceMutex.Unlock()

	if have {
		r.r.unloadDevice(deviceId, device)
	}
}

// Locate returns a processor for the given device,
// to either handle the request locally,
// or forward it on to the appropriate peer
func (r Router) Locate(deviceId uint64) (interface{ RUnlock() }, *grpc.ClientConn, bool, error) {
	return r.r.locate(deviceId, r.r.deviceCountChanged)
}
