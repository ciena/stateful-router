package routing

import (
	"context"
	"errors"
	"fmt"
	"github.com/golang/protobuf/ptypes/empty"
	"github.com/khagerma/stateful-experiment/protos/peer"
	"github.com/khagerma/stateful-experiment/protos/server"
	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/keepalive"
	"math"
	"net"
	"os"
	"sync"
	"time"
)

var (
	keepaliveTime        = time.Second * 10
	keepaliveTimeout     = time.Second * 5
	connectionMaxBackoff = time.Second * 5
	dnsPropagationDelay  = time.Second * 30

	waitReadyTime time.Duration
)

func init() {
	var err error
	if v, have := os.LookupEnv("ROUTING_KEEPALIVE_TIME"); have {
		if keepaliveTime, err = time.ParseDuration(v); err != nil {
			panic(err)
		}
	}
	if v, have := os.LookupEnv("ROUTING_KEEPALIVE_TIMEOUT"); have {
		if keepaliveTimeout, err = time.ParseDuration(v); err != nil {
			panic(err)
		}
	}
	if v, have := os.LookupEnv("ROUTING_CONNECTION_MAX_BACKOFF"); have {
		if connectionMaxBackoff, err = time.ParseDuration(v); err != nil {
			panic(err)
		}
	}
	if v, have := os.LookupEnv("ROUTING_DNS_PROPAGATION_DELAY"); have {
		if dnsPropagationDelay, err = time.ParseDuration(v); err != nil {
			panic(err)
		}
	}
	waitReadyTime = connectionMaxBackoff*2 + dnsPropagationDelay
}

type routingService struct {
	ordinal       uint32
	peerDNSFormat string

	server        *grpc.Server
	ctx           context.Context
	ctxCancelFunc context.CancelFunc

	peerMutex sync.RWMutex
	ready     bool
	readiness uint64
	peers     map[uint32]*node

	deviceMutex sync.RWMutex
	devices     map[uint64]*deviceData

	eventMutex                  sync.Mutex
	deviceCountEventData        deviceCountEventData
	deviceCountEventHandlerDone chan struct{}
	rebalanceEventData          rebalanceEventData
	rebalanceEventHandlerDone   chan struct{}

	implementation stateful.StatefulServer
}

type node struct {
	stateful.StatefulClient
	peer.PeerClient
	clientConn grpc.ClientConn
	connected  bool   // have I connected to this node
	ready      bool   // has this node informed me it's ready
	readiness  uint64 // which devices are ready
	devices    uint32 // number of devices this node is handling
}

type deviceData struct {
	mutex        sync.RWMutex
	loadingDone  chan struct{}
	loadingError error
}

func NewRoutingService(address, peerDNSFormat string, ordinal uint32, ss stateful.StatefulServer) stateful.StatefulServer {
	ctx, ctxCancelFunc := context.WithCancel(context.Background())
	router := &routingService{
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
		implementation:              ss,
	}

	for i := uint32(0); i < router.ordinal; i++ {
		router.connect(i)
	}

	time.AfterFunc(waitReadyTime, router.makeReady)
	go router.startStatsNotifier()
	go router.startRebalancer()

	listener, err := net.Listen("tcp", address)
	if err != nil {
		panic(fmt.Sprintf("failed to listen: %v", err))
	}

	router.server = grpc.NewServer(grpc.KeepaliveParams(
		keepalive.ServerParameters{
			Time:    keepaliveTime,
			Timeout: keepaliveTimeout,
		}), grpc.KeepaliveEnforcementPolicy(keepalive.EnforcementPolicy{
		MinTime:             keepaliveTime / 2,
		PermitWithoutStream: true,
	}))

	go router.start(listener)
	return router
}

func (router *routingService) start(lis net.Listener) {
	stateful.RegisterStatefulServer(router.server, router)
	peer.RegisterPeerServer(router.server, router)
	if err := router.server.Serve(lis); err != nil {
		panic(fmt.Sprintf("failed to serve: %v", err))
	}
}

func (router *routingService) Stop() {
	router.server.Stop()
	router.ctxCancelFunc()
	<-router.rebalanceEventHandlerDone
	<-router.deviceCountEventHandlerDone
}

func (router *routingService) makeReady() {
	fmt.Println("makeReady called")
	router.ready = true
	router.recalculateReadiness()
}

func (router *routingService) connect(ordinal uint32) {
	router.peerMutex.Lock()
	defer router.peerMutex.Unlock()

	if _, have := router.peers[ordinal]; !have {
		addr := fmt.Sprintf(router.peerDNSFormat, ordinal)
		cc, err := grpc.Dial(addr,
			grpc.WithInsecure(),
			grpc.WithBackoffConfig(grpc.BackoffConfig{MaxDelay: connectionMaxBackoff}),
			grpc.WithKeepaliveParams(keepalive.ClientParameters{
				Time:                keepaliveTime,
				Timeout:             keepaliveTimeout,
				PermitWithoutStream: true,
			}))
		if err != nil {
			panic(err)
		}

		router.peers[ordinal] = &node{
			StatefulClient: stateful.NewStatefulClient(cc),
			PeerClient:     peer.NewPeerClient(cc),
		}

		go router.watchState(cc, ordinal)
	}
}

func (router *routingService) watchState(cc *grpc.ClientConn, ordinal uint32) {
	state := connectivity.Connecting
	connectedAtLeastOnce := false
	for cc.WaitForStateChange(router.ctx, state) {
		lastState := state
		state = cc.GetState()

		if state == connectivity.Ready {
			if !connectedAtLeastOnce {
				connectedAtLeastOnce = true
				fmt.Println("Connected to node", ordinal)
			} else {
				fmt.Println("Reconnected to node", ordinal)
			}

			router.peerMutex.Lock()
			node := router.peers[ordinal]
			node.connected = true
			router.peerConnected(node)
			router.peerMutex.Unlock()

		} else if lastState == connectivity.Ready {
			// if the disconnected node has a greater ordinal than this one, just drop the connection
			fmt.Println("Connection to node", ordinal, "lost")
			router.peerMutex.Lock()
			if ordinal > router.ordinal {
				delete(router.peers, ordinal)
				router.recalculateReadiness()
				router.peerMutex.Unlock()
				break

			} else {
				node := router.peers[ordinal]
				node.connected = false
				node.ready = false
				node.readiness = 0
				node.devices = 0
				router.recalculateReadiness()
				router.peerMutex.Unlock()
			}
		}
	}
	cc.Close()
}

// -- implementations --

// peer interface impl

func (router *routingService) Hello(ctx context.Context, request *peer.HelloRequest) (*empty.Empty, error) {
	router.connect(request.Ordinal)
	return &empty.Empty{}, nil
}

func (router *routingService) UpdateStats(ctx context.Context, request *peer.StatsRequest) (*empty.Empty, error) {
	router.peerMutex.RLock()
	defer router.peerMutex.RUnlock()

	for ordinal, devices := range request.DeviceCounts {
		if node, have := router.peers[ordinal]; have {
			node.devices = devices
		}
	}
	router.recalculateReadiness()
	return &empty.Empty{}, nil
}

func (router *routingService) NextDevice(ctx context.Context, request *peer.NextDeviceRequest) (*peer.NextDeviceResponse, error) {
	router.deviceMutex.RLock()
	defer router.deviceMutex.RUnlock()

	migrateNext := uint64(math.MaxUint64)
	found := false
	first, isOnlyDeviceToMigrate := true, false
	for deviceId := range router.devices {
		// for every device that belongs on the other node
		if BestNode(deviceId, router.ordinal, map[uint32]struct{}{request.Ordinal: {}}) == request.Ordinal {
			// find the lowest device that's > other.readiness
			if deviceId > request.Readiness {
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

func (router *routingService) UpdateReadiness(ctx context.Context, request *peer.ReadinessRequest) (*empty.Empty, error) {
	fmt.Println("Node", request.Ordinal, "declared ready up to device", request.Readiness)
	router.peerMutex.Lock()
	recalculateReadiness := false
	if node, have := router.peers[request.Ordinal]; have {
		recalculateReadiness = !node.ready || node.readiness != request.Readiness
		node.ready = true
		node.readiness = request.Readiness
	}
	router.peerMutex.Unlock()

	router.deviceMutex.Lock()
	originalDeviceCount := uint32(len(router.devices))
	devicesToMove := make(map[uint64]*deviceData)
	for deviceId, device := range router.devices {
		//for every device that belongs on the other node
		if BestNode(deviceId, router.ordinal, map[uint32]struct{}{request.Ordinal: {}}) == request.Ordinal {
			// if the other node is ready for this device
			if deviceId <= request.Readiness {
				//release and notify that it's moved
				devicesToMove[deviceId] = device
				delete(router.devices, deviceId)
			}
		}
	}
	router.deviceMutex.Unlock()

	router.migrateDevices(devicesToMove, originalDeviceCount)

	if recalculateReadiness {
		router.recalculateReadiness()
	}

	return &empty.Empty{}, nil
}

func (router *routingService) migrateDevices(devicesToMove map[uint64]*deviceData, originalDeviceCount uint32) {
	if len(devicesToMove) != 0 {
		var ctr uint32
		for deviceId, device := range devicesToMove {
			ctr++
			router.migrateDevice(deviceId, device, originalDeviceCount-ctr)
		}
		router.deviceCountChanged()
	}
}

func (router *routingService) migrateDevice(deviceId uint64, device *deviceData, devices uint32) {
	// ensure the device has actually finished loading
	<-device.loadingDone
	if device.loadingError == nil {
		// wait for all in-progress requests to drain
		device.mutex.Lock()
		fmt.Println("Unlocking device", deviceId)
		router.implementation.Unlock(context.Background(), //always run to completion
			&stateful.UnlockRequest{Device: deviceId})
		device.mutex.Unlock()
	}

	router.Handoff(context.Background(), &peer.HandoffRequest{Device: deviceId, Ordinal: router.ordinal, Devices: devices})
}

// Handoff is just basically hint to load a device, so we'll do normal loading/locking
func (router *routingService) Handoff(ctx context.Context, request *peer.HandoffRequest) (*empty.Empty, error) {
	var peerUpdateComplete chan struct{}
	if device, remoteHandler, forward, err := router.handlerForInternal(request.Device, func() { peerUpdateComplete = router.deviceCountPeerChanged(request.Ordinal, request.Devices) }); err != nil {
		return &empty.Empty{}, err
	} else if forward {
		return remoteHandler.Handoff(ctx, request)
	} else {
		device.mutex.RUnlock()
		if peerUpdateComplete != nil {
			select {
			case <-peerUpdateComplete:
			case <-ctx.Done():
			}
		}
		return &empty.Empty{}, nil
	}
}

// stateful service interface impl

func (router *routingService) Lock(ctx context.Context, request *stateful.LockRequest) (*empty.Empty, error) {
	panic("not implemented")
}

func (router *routingService) Unlock(_ context.Context, request *stateful.UnlockRequest) (*empty.Empty, error) {
	panic("not implemented")
}

type statefulAndPeer interface {
	stateful.StatefulClient
	peer.PeerClient
}

// bestOfUnsafe returns which node this request should be routed to
// this takes into account node reachability & readiness
// router.peerMutex is assumed to be held by the caller
func (router *routingService) bestOfUnsafe(deviceId uint64) (uint32, error) {
	// build a list of possible nodes, including this one
	possibleNodes := make(map[uint32]struct{})
	if router.ready && router.readiness >= deviceId {
		possibleNodes[router.ordinal] = struct{}{}
	}
	for nodeId, node := range router.peers {
		if node.connected && node.ready && node.readiness >= deviceId {
			possibleNodes[nodeId] = struct{}{}
		}
	}

	if len(possibleNodes) == 0 {
		return 0, errors.New("no peers are ready to process this request")
	}

	// use the device ID to deterministically determine the best possible node
	return BestOf(deviceId, possibleNodes), nil
}

// handlerFor returns a processor for the given device,
// to either handle the request locally,
// or forward it on to the appropriate peer
func (router *routingService) handlerFor(deviceId uint64) (*deviceData, statefulAndPeer, bool, error) {
	return router.handlerForInternal(deviceId, router.deviceCountChanged)
}

func (router *routingService) handlerForInternal(deviceId uint64, deviceCountChangedCallback func()) (*deviceData, statefulAndPeer, bool, error) {
	router.deviceMutex.RLock()
	// if we have the device loaded, just process the request locally
	if device, have := router.devices[deviceId]; have {
		device.mutex.RLock()
		router.deviceMutex.RUnlock()
		<-device.loadingDone
		if device.loadingError != nil {
			device.mutex.RUnlock()
			return nil, nil, false, device.loadingError
		}
		return device, nil, false, nil
	}
	router.deviceMutex.RUnlock()

	router.peerMutex.RLock()
	// use the device ID to deterministically determine the best possible node
	node, err := router.bestOfUnsafe(deviceId)
	if err != nil {
		router.peerMutex.RUnlock()
		return nil, nil, false, err
	}
	// if we aren't the best node
	if node != router.ordinal {
		// send the request on to the best node
		client := router.peers[node]
		router.peerMutex.RUnlock()
		fmt.Println("Forwarding request to node", node)
		return nil, client, true, nil
	}
	router.peerMutex.RUnlock()
	// else, if this is the best node

	router.deviceMutex.Lock()
	if _, have := router.devices[deviceId]; have {
		// some other thread loaded the device since we last checked
		router.deviceMutex.Unlock()
		return router.handlerFor(deviceId)
	}
	device := &deviceData{
		loadingDone: make(chan struct{}),
	}
	device.mutex.RLock()
	router.devices[deviceId] = device
	deviceCountChangedCallback()
	router.deviceMutex.Unlock()

	fmt.Println("Locking device", deviceId)
	// have the implementation lock & load the device
	if _, err := router.implementation.Lock(context.Background(), &stateful.LockRequest{Device: deviceId}); err != nil {
		// failed to load the device, release it
		router.deviceMutex.Lock()
		if dev, have := router.devices[deviceId]; have && dev == device {
			delete(router.devices, deviceId)
			router.deviceCountChanged()
		}
		router.deviceMutex.Unlock()

		device.mutex.RUnlock()
		//inform any waiting threads that loading failed
		device.loadingError = err
		close(device.loadingDone)
		return nil, nil, false, err
	} else {
		close(device.loadingDone)

		// since we pre-RLocked this device, we already own it
		return device, nil, false, nil
	}
}

// stateful service pass-through (forward to appropriate node, or process locally)

func (router *routingService) SetData(ctx context.Context, request *stateful.SetDataRequest) (*empty.Empty, error) {
	if device, remoteHandler, forward, err := router.handlerFor(request.Device); err != nil {
		return &empty.Empty{}, err
	} else if forward {
		return remoteHandler.SetData(ctx, request)
	} else {
		defer device.mutex.RUnlock()
	}

	return router.implementation.SetData(ctx, request)
}

func (router *routingService) GetData(ctx context.Context, request *stateful.GetDataRequest) (*stateful.GetDataResponse, error) {
	if device, remoteHandler, forward, err := router.handlerFor(request.Device); err != nil {
		return &stateful.GetDataResponse{}, err
	} else if forward {
		return remoteHandler.GetData(ctx, request)
	} else {
		defer device.mutex.RUnlock()
	}

	return router.implementation.GetData(ctx, request)
}
