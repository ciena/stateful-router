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

var peerDNSFormat string
var listenAddress string

func init() {
	var have bool
	listenAddress, have = os.LookupEnv("LISTEN_ADDRESS")
	if !have {
		panic("env var LISTEN_ADDRESS not defined")
	}
	peerDNSFormat, have = os.LookupEnv("PEER_DNS_FORMAT")
	if !have {
		panic("env var PEER_DNS_FORMAT not specified")
	}
}

const (
	keepaliveTime        = time.Second * 10
	keepaliveTimeout     = time.Second * 5
	connectionMaxBackoff = time.Second * 5
	dnsPropagationDelay  = time.Second * 30

	waitReadyTime = connectionMaxBackoff*2 + dnsPropagationDelay
)

type routingService struct {
	ordinal uint32

	peerMutex sync.RWMutex
	ready     bool
	readiness uint64
	peers     map[uint32]*node

	deviceMutex sync.RWMutex
	devices     map[uint64]*deviceData

	implementation stateful.StatefulServer
}

type node struct {
	stateful.StatefulClient
	peer.PeerClient
	clientConn grpc.ClientConn
	connected  bool   // have I connected to this node
	ready      bool   // has this node informed me it's ready
	readiness  uint64 //which devices are ready
}

type deviceData struct {
	mutex        sync.RWMutex
	loadingDone  chan struct{}
	loadingError error
}

func NewRoutingService(ordinal uint32, ss stateful.StatefulServer) stateful.StatefulServer {
	ha := &routingService{
		ordinal:        ordinal,
		peers:          make(map[uint32]*node),
		devices:        make(map[uint64]*deviceData),
		implementation: ss,
	}

	go ha.start()
	return ha
}

func (router *routingService) start() {
	for i := uint32(0); i < router.ordinal; i++ {
		router.connect(i)
	}

	time.AfterFunc(waitReadyTime, router.makeReady)

	lis, err := net.Listen("tcp", listenAddress)
	if err != nil {
		panic(fmt.Sprintf("failed to listen: %v", err))
	}

	s := grpc.NewServer(grpc.KeepaliveParams(
		keepalive.ServerParameters{
			Time:    keepaliveTime,
			Timeout: keepaliveTimeout,
		}), grpc.KeepaliveEnforcementPolicy(keepalive.EnforcementPolicy{
		MinTime:             keepaliveTime / 2,
		PermitWithoutStream: true,
	}))
	stateful.RegisterStatefulServer(s, router)
	peer.RegisterPeerServer(s, router)
	if err := s.Serve(lis); err != nil {
		panic(fmt.Sprintf("failed to serve: %v", err))
	}
}

func (router *routingService) makeReady() {
	fmt.Println("Ready - migrating devices")
	readiness := uint64(0)
	for {

		router.peerMutex.Lock()
		// increment requests that we will accept locally
		router.ready = true
		router.readiness = readiness

		toNotify := make([]peer.PeerClient, 0, len(router.peers))
		for _, con := range router.peers {
			if con.connected {
				toNotify = append(toNotify, con.PeerClient)
			}
		}
		router.peerMutex.Unlock()

		// keep nodes informed on how ready we are
		next := uint64(math.MaxUint64)
		for _, client := range toNotify {
			// inform peers that we're ready for devices
			resp, err := client.Ready(context.Background(), &peer.ReadyRequest{Ordinal: router.ordinal, Readiness: readiness})
			if err == nil {
				// determine which device is next in line for migration
				if resp.NextDeviceToMigrate < next {
					next = resp.NextDeviceToMigrate
				}
			}
		}

		//when we reach 100% readiness, we're done
		if readiness == math.MaxUint64 {
			break
		}
		readiness = next
	}
	fmt.Println("Done migrating")
}

func (router *routingService) connect(ordinal uint32) {
	router.peerMutex.Lock()
	defer router.peerMutex.Unlock()

	if _, have := router.peers[ordinal]; !have {
		addr := fmt.Sprintf(peerDNSFormat, ordinal)
		fmt.Println("Connecting to node", ordinal, "at", addr)
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
	for cc.WaitForStateChange(context.Background(), state) {
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
			router.peers[ordinal].connected = true
			router.peerMutex.Unlock()

			_, err := peer.NewPeerClient(cc).Hello(context.Background(), &peer.HelloRequest{Ordinal: router.ordinal})

			if err == nil {
				router.peerMutex.RLock()
				readiness := router.readiness
				router.peerMutex.RUnlock()
				if readiness != 0 {
					peer.NewPeerClient(cc).Ready(context.Background(), &peer.ReadyRequest{Ordinal: router.ordinal, Readiness: readiness})
				}
			}
		} else if lastState == connectivity.Ready {
			// if the disconnected node has a greater ordinal than this one, just drop the connection
			fmt.Println("Connection to node", ordinal, "lost")
			router.peerMutex.Lock()
			if ordinal > router.ordinal {
				delete(router.peers, ordinal)
				router.peerMutex.Unlock()
				break

			} else {
				node := router.peers[ordinal]
				node.connected = false
				node.ready = false
				node.readiness = 0
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

func (router *routingService) Ready(ctx context.Context, request *peer.ReadyRequest) (*peer.ReadyResponse, error) {
	fmt.Println("Node", request.Ordinal, "declared ready up to device", request.Readiness)
	router.peerMutex.Lock()
	node := router.peers[request.Ordinal]
	node.ready = true
	node.readiness = request.Readiness
	router.peerMutex.Unlock()

	router.deviceMutex.Lock()
	devicesToMove := make(map[uint64]*deviceData)
	//for every device that belongs on the other node
	migrateNext := uint64(math.MaxUint64)
	for deviceId, device := range router.devices {
		//for every device that belongs on the other node
		if BestNode(deviceId, router.ordinal, map[uint32]struct{}{request.Ordinal: {}}) == request.Ordinal {
			// if the other node is ready for this device
			if deviceId <= request.Readiness {
				fmt.Println("will migrate device", deviceId)
				//release and notify that it's moved
				devicesToMove[deviceId] = device
				delete(router.devices, deviceId)
			} else {
				// if the other node is not ready for the device, consider it for the next move
				if deviceId < migrateNext {
					migrateNext = deviceId
				}
			}
		}
	}
	router.deviceMutex.Unlock()

	// migrate devices
	for deviceId, device := range devicesToMove {
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

		router.Handoff(context.Background(), &peer.HandoffRequest{Device: deviceId})
	}

	return &peer.ReadyResponse{NextDeviceToMigrate: migrateNext}, nil
}

// Handoff is just a hint to load a device, so we'll do normal loading/locking
func (router *routingService) Handoff(ctx context.Context, request *peer.HandoffRequest) (*empty.Empty, error) {
	if device, remoteHandler, useLocal, err := router.handlerFor(request.Device); err != nil {
		return &empty.Empty{}, err
	} else if useLocal {
		defer device.mutex.RUnlock()
	} else {
		remoteHandler.Handoff(ctx, request)
	}
	return &empty.Empty{}, nil
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

// handlerFor returns a processor for the given device,
// to either handle the request locally,
// or forward it on to the appropriate peer
func (router *routingService) handlerFor(deviceId uint64) (*deviceData, statefulAndPeer, bool, error) {
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
		return device, nil, true, nil
	}
	router.deviceMutex.RUnlock()

	// build a list of possible nodes, including this one
	possibleNodes := make(map[uint32]struct{})
	router.peerMutex.RLock()
	if router.ready && router.readiness >= deviceId {
		possibleNodes[router.ordinal] = struct{}{}
	}
	for nodeId, node := range router.peers {
		if node.connected && node.ready && node.readiness >= deviceId {
			possibleNodes[nodeId] = struct{}{}
		}
	}

	if len(possibleNodes) == 0 {
		router.peerMutex.RUnlock()
		return nil, nil, false, errors.New("no peers are ready to process this request")
	}

	// use the device ID to deterministically determine the best possible node
	node := BestOf(deviceId, possibleNodes)
	// if we aren't the best node
	if node != router.ordinal {
		// send the request on to the best node
		client := router.peers[node]
		router.peerMutex.RUnlock()
		fmt.Println("Forwarding request to node", node)
		return nil, client, false, nil
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
	router.devices[deviceId] = device
	router.deviceMutex.Unlock()

	fmt.Println("Locking device", deviceId)
	// have the implementation lock & load the device
	if _, err := router.implementation.Lock(context.Background(), &stateful.LockRequest{Device: deviceId}); err != nil {
		// failed to load the device, release it
		router.deviceMutex.Lock()
		if dev, have := router.devices[deviceId]; have && dev == device {
			delete(router.devices, deviceId)
		}
		router.deviceMutex.Unlock()

		//inform any waiting threads that loading failed
		device.loadingError = err
		close(device.loadingDone)
		return nil, nil, false, err
	} else {
		close(device.loadingDone)

		// after loading the device, restart routing, as things may have changed in the time it took to load
		return router.handlerFor(deviceId)
	}
}

// stateful service pass-through (forward to appropriate node, or process locally)

func (router *routingService) SetData(ctx context.Context, request *stateful.SetDataRequest) (*empty.Empty, error) {
	if device, remoteHandler, useLocal, err := router.handlerFor(request.Device); err != nil {
		return &empty.Empty{}, err
	} else if useLocal {
		defer device.mutex.RUnlock()

		return router.implementation.SetData(ctx, request)
	} else {
		return remoteHandler.SetData(ctx, request)
	}
}

func (router *routingService) GetData(ctx context.Context, request *stateful.GetDataRequest) (*stateful.GetDataResponse, error) {
	if device, remoteHandler, useLocal, err := router.handlerFor(request.Device); err != nil {
		return &stateful.GetDataResponse{}, err
	} else if useLocal {
		defer device.mutex.RUnlock()

		return router.implementation.GetData(ctx, request)
	} else {
		return remoteHandler.GetData(ctx, request)
	}
}
