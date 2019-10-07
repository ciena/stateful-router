package router

import (
	"context"
	"errors"
	"fmt"
	"github.com/kent-h/stateful-router/protos/peer"
	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/keepalive"
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

type Router struct {
	// every instance of Router in the cluster has a unique ordinal ID,
	// these should be sequentially assigned, starting from zero
	// intended to be used with a k8s StatefulSet
	ordinal uint32
	// mapping from ordinal to address or DNS entry
	peerDNSFormat string

	// server accepts requests from peers
	server *grpc.Server
	// closed only when the Router is stopping
	ctx           context.Context
	ctxCancelFunc context.CancelFunc

	// readiness and maxReadiness indicates which devices (if any) this node doesn't have responsibility for (devices < readiness),
	// it is owned by (and should only be changed by) the startRebalancer() thread
	readiness                  uint64
	maxReadiness               bool
	decreasingFromMaxReadiness bool

	// peerMutex synchronizes all access to the peers,
	// it also guards access to readiness/maxReadiness for non- startRebalancer() threads
	peerMutex sync.RWMutex
	peers     map[uint32]*node

	// deviceMutex synchronizes all access to devices
	deviceMutex sync.RWMutex
	devices     map[uint64]*deviceData

	// events are sent to startRebalancer() and startStatsNotifier()
	eventMutex           sync.Mutex
	deviceCountEventData deviceCountEventData
	rebalanceEventData   rebalanceEventData
	// when shutting down, startRebalancer() and startStatsNotifier() close these channels
	deviceCountEventHandlerDone chan struct{}
	rebalanceEventHandlerDone   chan struct{}

	loader DeviceLoader
}

type node struct {
	clientConn *grpc.ClientConn
	peer.PeerClient
	connected    bool   // have I connected to this node
	readiness    uint64 // which devices are ready
	maxReadiness bool
	devices      uint32 // number of devices this node is handling
}

type deviceData struct {
	// mutex is read-locked for the duration of every request,
	// the device will not be unlocked/migrated until all requests release it
	mutex sync.RWMutex
	// closed only after the device lock attempt is completed (and loadingError is set)
	loadingDone  chan struct{}
	loadingError error
}

func (router *Router) connect(ordinal uint32) {
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

		node := &node{
			clientConn: cc,
			PeerClient: peer.NewPeerClient(cc),
		}
		router.peers[ordinal] = node

		go router.watchState(cc, node, ordinal)
	}
}

func (router *Router) watchState(cc *grpc.ClientConn, node *node, nodeId uint32) {
	state := connectivity.Connecting
	connectedAtLeastOnce := false
	for cc.WaitForStateChange(router.ctx, state) {
		lastState := state
		state = cc.GetState()

		if state == connectivity.Ready {
			if !connectedAtLeastOnce {
				connectedAtLeastOnce = true
				fmt.Println("Connected to node", nodeId)
			} else {
				fmt.Println("Reconnected to node", nodeId)
			}

			if _, err := node.Hello(router.ctx, &peer.HelloRequest{Ordinal: router.ordinal}); err != nil {
				fmt.Println(err)
			}

			router.peerMutex.Lock()
			node.connected = true
			router.peerConnected(node)
			router.peerMutex.Unlock()

		} else if lastState == connectivity.Ready {
			fmt.Println("Connection to node", nodeId, "lost")
			router.peerMutex.Lock()
			if nodeId > router.ordinal {
				// if the disconnected node has a greater ordinal than this one, just drop the connection
				delete(router.peers, nodeId)
				router.rebalance()
				router.peerMutex.Unlock()
				break

			} else {
				node.connected = false
				node.readiness, node.maxReadiness = 0, false
				node.devices = 0
				router.rebalance()
				router.peerMutex.Unlock()
			}
		}
	}
	cc.Close()
}

func (router *Router) migrateDevices(devicesToMove map[uint64]*deviceData, originalDeviceCount uint32) {
	if len(devicesToMove) != 0 {
		var ctr uint32
		for deviceId, device := range devicesToMove {
			ctr++
			router.migrateDevice(deviceId, device, originalDeviceCount-ctr)
		}
		router.deviceCountChanged()
	}
}

func (router *Router) migrateDevice(deviceId uint64, device *deviceData, devices uint32) {
	router.unloadDevice(deviceId, device)

	peerApi := peerApi{router}
	if _, err := peerApi.Handoff(router.ctx, &peer.HandoffRequest{
		Device:  deviceId,
		Ordinal: router.ordinal,
		Devices: devices,
	}); err != nil {
		fmt.Println(err)
	}
}

func (router *Router) unloadDevice(deviceId uint64, device *deviceData) {
	// ensure the device has actually finished loading
	<-device.loadingDone
	if device.loadingError == nil {
		// wait for all in-progress requests to drain
		device.mutex.Lock()
		fmt.Printf("Unloading device %016x\n", deviceId)
		router.loader.Unload(deviceId)
		device.mutex.Unlock()
	}
}

// bestOfUnsafe returns which node this request should be routed to
// this takes into account node reachability & readiness
// router.peerMutex is assumed to be held by the caller
func (router *Router) bestOfUnsafe(deviceId uint64) (uint32, error) {
	// build a list of possible nodes, including this one
	possibleNodes := make(map[uint32]struct{})
	if deviceId < router.readiness || router.maxReadiness {
		possibleNodes[router.ordinal] = struct{}{}
	}
	for nodeId, node := range router.peers {
		if node.connected && (deviceId < node.readiness || node.maxReadiness) {
			possibleNodes[nodeId] = struct{}{}
		}
	}

	if len(possibleNodes) == 0 {
		return 0, errors.New("no peers are ready to process this request")
	}

	// use the device ID to deterministically determine the best possible node
	return BestOf(deviceId, possibleNodes), nil
}

func (router *Router) locate(deviceId uint64, deviceCountChangedCallback func()) (interface{ RUnlock() }, *grpc.ClientConn, bool, error) {
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
		return &device.mutex, nil, false, nil
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
		return nil, client.clientConn, true, nil
	}
	router.peerMutex.RUnlock()
	// else, if this is the best node

	router.deviceMutex.Lock()
	if _, have := router.devices[deviceId]; have {
		// some other thread loaded the device since we last checked
		router.deviceMutex.Unlock()
		return router.locate(deviceId, deviceCountChangedCallback)
	}
	device := &deviceData{
		loadingDone: make(chan struct{}),
	}
	device.mutex.RLock()
	router.devices[deviceId] = device
	deviceCountChangedCallback()
	router.deviceMutex.Unlock()

	fmt.Printf("Loading device %016x\n", deviceId)
	// have the implementation lock & load the device
	if err := router.loader.Load(router.ctx, deviceId); err != nil {
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
		return &device.mutex, nil, false, nil
	}
}
