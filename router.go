package router

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/kent-h/stateful-router/protos/peer"
	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/keepalive"
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
	eventMutex             sync.Mutex
	resourceCountEventData resourceCountEventData
	rebalanceEventData     rebalanceEventData
	// triggers clean shutdown
	handoffAndShutdown chan struct{}
	// when shutting down, startRebalancer() and startStatsNotifier() close these channels
	statusNotifierDone chan struct{}
	rebalancerDone     chan struct{}

	loader Loader
}

type ResourceType uint32

type resourceData struct {
	// readiness, readyForEqual, and readinessMax indicates which resources (if any) this node doesn't have responsibility for (resources < readiness),
	// it is owned by (and should only be changed by) the startRebalancer() thread
	readiness                  string
	readyForEqual              bool
	readinessMax               bool
	decreasingFromMaxReadiness bool

	// mutex synchronizes all access to resources
	mutex  sync.RWMutex
	loaded map[string]*syncher
}

type node struct {
	clientConn *grpc.ClientConn
	peer.PeerClient
	connected    bool // have I connected to this node
	shuttingDown bool

	// resource-specific information
	resources map[ResourceType]metadata
}

type metadata struct {
	readiness     string // which resources are ready
	readyForEqual bool
	readinessMax  bool

	count uint32 // number of resources this node is handling
}

type syncher struct {
	// mutex is read-locked for the duration of every request,
	// the instance will not be unlocked/migrated until all requests release it
	mutex sync.RWMutex
	// closed only after the instance lock attempt is completed (and loadingError is set)
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
			resources:  make(map[ResourceType]metadata),
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
				node.resources = make(map[ResourceType]metadata)
				router.rebalanceAll()
				router.peerMutex.Unlock()
			}
		}
	}
	cc.Close()
}

func (router *Router) migrateResources(resourceType ResourceType, resourcesToMove map[string]*syncher, originalDeviceCount uint32) {
	if len(resourcesToMove) != 0 {
		var ctr uint32
		for resourceId, syncher := range resourcesToMove {
			ctr++
			router.migrateResource(resourceType, resourceId, syncher, originalDeviceCount-ctr)
		}
		router.resourceCountChanged(resourceType)
	}
}

func (router *Router) migrateResource(resourceType ResourceType, resourceId string, syncher *syncher, resourceCount uint32) {
	router.unloadResource(resourceType, resourceId, syncher, router.loader.Unload)

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

func (router *Router) unloadResource(resourceType ResourceType, resourceId string, syncher *syncher, unloadFunc func(resourceType ResourceType, deviceId string)) {
	// ensure the resource has actually finished loading
	<-syncher.loadingDone
	if syncher.loadingError == nil {
		// wait for all in-progress requests to drain
		syncher.mutex.Lock()
		defer syncher.mutex.Unlock()

		fmt.Printf("Unloading resource %s\n", strconv.Quote(resourceId))
		unloadFunc(resourceType, resourceId)
	}
}

// bestOfUnsafe returns which node this request should be routed to
// this takes into account node reachability & readiness
// router.peerMutex is assumed to be held by the caller
func (router *Router) bestOfUnsafe(resource *resourceData, resourceType ResourceType, resourceId string) (uint32, error) {
	// build a list of possible nodes, including this one
	possibleNodes := make(map[uint32]struct{})
	if resourceId < resource.readiness || (resourceId == resource.readiness && resource.readyForEqual) || resource.readinessMax {
		possibleNodes[router.ordinal] = struct{}{}
	}
	for nodeId, node := range router.peers {
		nodeData := node.resources[resourceType]
		if node.connected && (resourceId < nodeData.readiness || (resourceId == nodeData.readiness && nodeData.readyForEqual) || nodeData.readinessMax) {
			possibleNodes[nodeId] = struct{}{}
		}
	}

	if len(possibleNodes) == 0 {
		return 0, errors.New("no peers are ready to process this request")
	}

	// use the resource ID to deterministically determine the best possible node
	return BestOf(resourceId, possibleNodes), nil
}

func (router *Router) locate(resourceType ResourceType, resourceId string, resourceCountChangedCallback func(resourceType ResourceType), loadFunc func(ctx context.Context, resourceType ResourceType, resourceId string) error) (interface{ RUnlock() }, *grpc.ClientConn, bool, error) {
	resource, have := router.resources[resourceType]
	if !have {
		return nil, nil, false, fmt.Errorf(errorResourceTypeNotFound, resourceType)
	}

	resource.mutex.RLock()
	// if we have the resource loaded, just process the request locally
	if instance, have := resource.loaded[resourceId]; have {
		instance.mutex.RLock()
		resource.mutex.RUnlock()
		<-instance.loadingDone
		if instance.loadingError != nil {
			instance.mutex.RUnlock()
			return nil, nil, false, instance.loadingError
		}
		return &instance.mutex, nil, false, nil
	}
	resource.mutex.RUnlock()

	router.peerMutex.RLock()
	// use the resource ID to deterministically determine the best possible node
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

	resource.mutex.Lock()
	if _, have := resource.loaded[resourceId]; have {
		// some other thread loaded the resource since we last checked
		resource.mutex.Unlock()
		return router.locate(resourceType, resourceId, resourceCountChangedCallback, loadFunc)
	}
	instance := &syncher{
		loadingDone: make(chan struct{}),
	}
	instance.mutex.RLock()
	resource.loaded[resourceId] = instance
	resourceCountChangedCallback(resourceType)
	resource.mutex.Unlock()

	fmt.Printf("Loading resource %s\n", strconv.Quote(resourceId))
	// have the implementation lock & load the resource
	if err := loadFunc(router.ctx, resourceType, resourceId); err != nil {
		// failed to load the resource, release it
		resource.mutex.Lock()
		if dev, have := resource.loaded[resourceId]; have && dev == instance {
			delete(resource.loaded, resourceId)
			router.resourceCountChanged(resourceType)
		}
		resource.mutex.Unlock()

		instance.mutex.RUnlock()
		//inform any waiting threads that loading failed
		instance.loadingError = err
		close(instance.loadingDone)
		return nil, nil, false, err
	} else {
		close(instance.loadingDone)

		// since we pre-RLocked this resource, we already own it
		return &instance.mutex, nil, false, nil
	}
}
