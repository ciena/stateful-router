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

	// node will only start accepting requests after rebalancer is started (default is to accepting no requests)
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

// HandlerFor returns a processor for the given device,
// to either handle the request locally,
// or forward it on to the appropriate peer
func (r Router) HandlerFor(deviceId uint64) (interface{ RUnlock() }, *grpc.ClientConn, bool, error) {
	return r.r.handlerFor(deviceId, r.r.deviceCountChanged)
}
