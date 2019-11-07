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
	"strconv"
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

	// closed only when the Router is stopping
	ctx context.Context

	// resource maintains resource-specific data
	// since it is only set at startup, it is safe to read without a lock
	resources map[ResourceType]*resourceData

	// peerMutex synchronizes all access to the peers,
	// it also guards access to readiness/readinessMax for non- startRebalancer() threads
	peerMutex sync.RWMutex
	peers     map[uint32]*node

	// events are sent to startRebalancer() and startStatsNotifier()
	eventMutex           sync.Mutex
	deviceCountEventData deviceCountEventData
	rebalanceEventData   rebalanceEventData
	// triggers clean shutdown
	handoffAndShutdown chan struct{}
	// when shutting down, startRebalancer() and startStatsNotifier() close these channels
	deviceCountEventHandlerDone chan struct{}
	rebalanceEventHandlerDone   chan struct{}

	loader DeviceLoader
}

type ResourceType uint32

type resourceData struct {
	// readiness, readyForEqual, and readinessMax indicates which resources (if any) this node doesn't have responsibility for (resources < readiness),
	// it is owned by (and should only be changed by) the startRebalancer() thread
	readiness                  string
	readyForEqual              bool
	readinessMax               bool
	decreasingFromMaxReadiness bool

	// deviceMutex synchronizes all access to resources
	deviceMutex sync.RWMutex
	devices     map[string]*deviceData
}

type node struct {
	clientConn *grpc.ClientConn
	peer.PeerClient
	connected    bool // have I connected to this node
	shuttingDown bool

	// resource-specific information
	resources map[ResourceType]nodeResourceData
}

type nodeResourceData struct {
	readiness     string // which resources are ready
	readyForEqual bool
	readinessMax  bool

	count uint32 // number of resources this node is handling
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
			resources:  make(map[ResourceType]nodeResourceData),
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
				router.rebalanceAll()
				router.peerMutex.Unlock()
				break

			} else {
				node.connected = false
				node.shuttingDown = false
				node.resources = make(map[ResourceType]nodeResourceData)
				router.rebalanceAll()
				router.peerMutex.Unlock()
			}
		}
	}
	cc.Close()
}

func (router *Router) migrateResources(resourceType ResourceType, resourcesToMove map[string]*deviceData, originalDeviceCount uint32) {
	if len(resourcesToMove) != 0 {
		var ctr uint32
		for deviceId, device := range resourcesToMove {
			ctr++
			router.migrateResource(resourceType, deviceId, device, originalDeviceCount-ctr)
		}
		router.deviceCountChanged(resourceType)
	}
}

func (router *Router) migrateResource(resourceType ResourceType, resourceId string, device *deviceData, resourceCount uint32) {
	router.unloadDevice(resourceType, resourceId, device, router.loader.Unload)

	peerApi := peerApi{router}
	if _, err := peerApi.Handoff(router.ctx, &peer.HandoffRequest{
		ResourceType:  uint32(resourceType),
		ResourceId:    []byte(resourceId),
		Ordinal:       router.ordinal,
		ResourceCount: resourceCount,
	}); err != nil {
		fmt.Println(fmt.Errorf("error during handoff: %s", err))
	}
}

func (router *Router) unloadDevice(resourceType ResourceType, deviceId string, device *deviceData, unloadFunc func(resourceType ResourceType, deviceId string)) {
	// ensure the device has actually finished loading
	<-device.loadingDone
	if device.loadingError == nil {
		// wait for all in-progress requests to drain
		device.mutex.Lock()
		defer device.mutex.Unlock()

		fmt.Printf("Unloading device %s\n", strconv.Quote(deviceId))
		unloadFunc(resourceType, deviceId)
	}
}

// bestOfUnsafe returns which node this request should be routed to
// this takes into account node reachability & readiness
// router.peerMutex is assumed to be held by the caller
func (router *Router) bestOfUnsafe(resource *resourceData, resourceType ResourceType, deviceId string) (uint32, error) {
	// build a list of possible nodes, including this one
	possibleNodes := make(map[uint32]struct{})
	if deviceId < resource.readiness || (deviceId == resource.readiness && resource.readyForEqual) || resource.readinessMax {
		possibleNodes[router.ordinal] = struct{}{}
	}
	for nodeId, node := range router.peers {
		nodeData := node.resources[resourceType]
		if node.connected && (deviceId < nodeData.readiness || (deviceId == nodeData.readiness && nodeData.readyForEqual) || nodeData.readinessMax) {
			possibleNodes[nodeId] = struct{}{}
		}
	}

	if len(possibleNodes) == 0 {
		return 0, errors.New("no peers are ready to process this request")
	}

	// use the device ID to deterministically determine the best possible node
	return BestOf(deviceId, possibleNodes), nil
}

func (router *Router) locate(resourceType ResourceType, resourceId string, deviceCountChangedCallback func(resourceType ResourceType), loadFunc func(ctx context.Context, resourceType ResourceType, resourceId string) error) (interface{ RUnlock() }, *grpc.ClientConn, bool, error) {
	resource, have := router.resources[resourceType]
	if !have {
		return nil, nil, false, fmt.Errorf(errorResourceTypeNotFound, resourceType)
	}

	resource.deviceMutex.RLock()
	// if we have the device loaded, just process the request locally
	if device, have := resource.devices[resourceId]; have {
		device.mutex.RLock()
		resource.deviceMutex.RUnlock()
		<-device.loadingDone
		if device.loadingError != nil {
			device.mutex.RUnlock()
			return nil, nil, false, device.loadingError
		}
		return &device.mutex, nil, false, nil
	}
	resource.deviceMutex.RUnlock()

	router.peerMutex.RLock()
	// use the device ID to deterministically determine the best possible node
	node, err := router.bestOfUnsafe(resource, resourceType, resourceId)
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

	resource.deviceMutex.Lock()
	if _, have := resource.devices[resourceId]; have {
		// some other thread loaded the device since we last checked
		resource.deviceMutex.Unlock()
		return router.locate(resourceType, resourceId, deviceCountChangedCallback, loadFunc)
	}
	device := &deviceData{
		loadingDone: make(chan struct{}),
	}
	device.mutex.RLock()
	resource.devices[resourceId] = device
	deviceCountChangedCallback(resourceType)
	resource.deviceMutex.Unlock()

	fmt.Printf("Loading device %s\n", strconv.Quote(resourceId))
	// have the implementation lock & load the device
	if err := loadFunc(router.ctx, resourceType, resourceId); err != nil {
		// failed to load the device, release it
		resource.deviceMutex.Lock()
		if dev, have := resource.devices[resourceId]; have && dev == device {
			delete(resource.devices, resourceId)
			router.deviceCountChanged(resourceType)
		}
		resource.deviceMutex.Unlock()

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
