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

func addressFromOrdinal(ordinal uint32) string {

	return fmt.Sprintf(peerDNSFormat, ordinal)
}

const maxBackoff = time.Second * 4
const waitReadyTime = time.Second * 5

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

func (ha *routingService) start() {
	for i := uint32(0); i < ha.ordinal; i++ {
		ha.connect(i)
	}

	//wait a bit, for connections to be established
	time.AfterFunc(waitReadyTime, ha.makeReady)

	lis, err := net.Listen("tcp", listenAddress)
	if err != nil {
		panic(fmt.Sprintf("failed to listen: %v", err))
	}

	s := grpc.NewServer(grpc.KeepaliveParams(keepalive.ServerParameters{Time: time.Second * 2, Timeout: time.Second * 2}))
	stateful.RegisterStatefulServer(s, ha)
	peer.RegisterPeerServer(s, ha)
	if err := s.Serve(lis); err != nil {
		panic(fmt.Sprintf("failed to serve: %v", err))
	}
}

func (ha *routingService) makeReady() {
	fmt.Println("Ready - migrating devices")
	readiness := uint64(0)
	for {

		ha.peerMutex.Lock()
		// increment requests that we will accept locally
		ha.ready = true
		ha.readiness = readiness

		toNotify := make([]peer.PeerClient, 0, len(ha.peers))
		for _, con := range ha.peers {
			if con.connected {
				toNotify = append(toNotify, con.PeerClient)
			}
		}
		ha.peerMutex.Unlock()

		// keep nodes informed on how ready we are
		next := uint64(math.MaxUint64)
		for _, client := range toNotify {
			// inform peers that we're ready for devices
			resp, err := client.Ready(context.Background(), &peer.ReadyRequest{Ordinal: ha.ordinal, Readiness: readiness})
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

func (ha *routingService) connect(ordinal uint32) {
	ha.peerMutex.Lock()
	defer ha.peerMutex.Unlock()

	if _, have := ha.peers[ordinal]; !have {
		addr := addressFromOrdinal(ordinal)
		fmt.Println("Connecting to node", ordinal, "at", addr)
		cc, err := grpc.Dial(addr, grpc.WithInsecure(), grpc.WithBackoffConfig(grpc.BackoffConfig{MaxDelay: maxBackoff}))
		if err != nil {
			panic(err)
		}

		ha.peers[ordinal] = &node{
			StatefulClient: stateful.NewStatefulClient(cc),
			PeerClient:     peer.NewPeerClient(cc),
		}

		go ha.watchState(cc, ordinal)
	}
}

func (ha *routingService) watchState(cc *grpc.ClientConn, ordinal uint32) {
	state := connectivity.Connecting
	for cc.WaitForStateChange(context.Background(), state) {
		lastState := state
		state = cc.GetState()

		if state == connectivity.Ready {
			ha.peerMutex.Lock()
			ha.peers[ordinal].connected = true
			ha.peerMutex.Unlock()

			_, err := peer.NewPeerClient(cc).Hello(context.Background(), &peer.HelloRequest{Ordinal: ha.ordinal})

			if err == nil {
				ha.peerMutex.RLock()
				readiness := ha.readiness
				ha.peerMutex.RUnlock()
				if readiness != 0 {
					peer.NewPeerClient(cc).Ready(context.Background(), &peer.ReadyRequest{Ordinal: ha.ordinal, Readiness: readiness})
				}
			}
		} else if lastState == connectivity.Ready {
			// if the disconnected node has a greater ordinal than this one, just drop the connection
			fmt.Println("Connection to node", ordinal, "lost")
			ha.peerMutex.Lock()
			if ordinal > ha.ordinal {
				delete(ha.peers, ordinal)
				ha.peerMutex.Unlock()
				break

			} else {
				node := ha.peers[ordinal]
				node.connected = false
				node.ready = false
				node.readiness = 0
				ha.peerMutex.Unlock()
			}
		}
	}
	cc.Close()
}

// -- implementations --

// peer interface impl

func (ha *routingService) Hello(ctx context.Context, request *peer.HelloRequest) (*empty.Empty, error) {
	ha.connect(request.Ordinal)
	return &empty.Empty{}, nil
}

func (ha *routingService) Ready(ctx context.Context, request *peer.ReadyRequest) (*peer.ReadyResponse, error) {
	fmt.Println("Node", request.Ordinal, "declared ready up to device", request.Readiness)
	ha.peerMutex.Lock()
	node := ha.peers[request.Ordinal]
	node.ready = true
	node.readiness = request.Readiness
	ha.peerMutex.Unlock()

	ha.deviceMutex.Lock()
	devicesToMove := make(map[uint64]*deviceData)
	//for every device that belongs on the other node
	migrateNext := uint64(math.MaxUint64)
	for deviceId, device := range ha.devices {
		//for every device that belongs on the other node
		if BestNode(deviceId, ha.ordinal, map[uint32]struct{}{request.Ordinal: {}}) == request.Ordinal {
			// if the other node is ready for this device
			if deviceId <= request.Readiness {
				fmt.Println("will migrate device", deviceId)
				//release and notify that it's moved
				devicesToMove[deviceId] = device
				delete(ha.devices, deviceId)
			} else {
				// if the other node is not ready for the device, consider it for the next move
				if deviceId < migrateNext {
					migrateNext = deviceId
				}
			}
		}
	}
	ha.deviceMutex.Unlock()

	// migrate devices
	for deviceId, device := range devicesToMove {
		// ensure the device has actually finished loading
		<-device.loadingDone
		if device.loadingError == nil {
			// wait for all in-progress requests to drain
			device.mutex.Lock()
			fmt.Println("Unlocking device", deviceId)
			ha.implementation.Unlock(context.Background(), //always run to completion
				&stateful.UnlockRequest{Device: deviceId})
			device.mutex.Unlock()
		}

		ha.Handoff(context.Background(), &peer.HandoffRequest{Device: deviceId})
	}

	return &peer.ReadyResponse{NextDeviceToMigrate: migrateNext}, nil
}

// Handoff is just a hint to load a device, so we'll do normal loading/locking
func (ha *routingService) Handoff(ctx context.Context, request *peer.HandoffRequest) (*empty.Empty, error) {
	if device, remoteHandler, useLocal, err := ha.handlerFor(request.Device); err != nil {
		return &empty.Empty{}, err
	} else if useLocal {
		defer device.mutex.RUnlock()
	} else {
		remoteHandler.Handoff(ctx, request)
	}
	return &empty.Empty{}, nil
}

// stateful service interface impl

func (ha *routingService) Lock(ctx context.Context, request *stateful.LockRequest) (*empty.Empty, error) {
	panic("not implemented")
}

func (ha *routingService) Unlock(_ context.Context, request *stateful.UnlockRequest) (*empty.Empty, error) {
	panic("not implemented")
}

type statefulAndPeer interface {
	stateful.StatefulClient
	peer.PeerClient
}

// handlerFor returns a processor for the given device,
// to either handles the request locally,
// or forward it on to the appropriate peer
func (ha *routingService) handlerFor(deviceId uint64) (*deviceData, statefulAndPeer, bool, error) {
	ha.deviceMutex.RLock()
	device, have := ha.devices[deviceId]
	// if we have the device loaded, just process the request locally
	if have {
		device.mutex.RLock()
		ha.deviceMutex.RUnlock()
		<-device.loadingDone
		if device.loadingError != nil {
			device.mutex.RUnlock()
			return nil, nil, false, device.loadingError
		}
		return device, nil, true, nil
	}
	ha.deviceMutex.RUnlock()

	// build a list of possible nodes, including this one
	possibleNodes := make(map[uint32]struct{})
	ha.peerMutex.RLock()
	if ha.ready && ha.readiness >= deviceId {
		possibleNodes[ha.ordinal] = struct{}{}
	}
	for nodeId, node := range ha.peers {
		if node.connected && node.ready && node.readiness >= deviceId {
			possibleNodes[nodeId] = struct{}{}
		}
	}

	if len(possibleNodes) == 0 {
		ha.peerMutex.RUnlock()
		return nil, nil, false, errors.New("no peers are ready to process this request")
	}

	// use the device ID to deterministically determine the best possible node
	node := BestOf(deviceId, possibleNodes)
	// if we aren't the best node
	if node != ha.ordinal {
		// send the request on to the best node
		client := ha.peers[node]
		ha.peerMutex.RUnlock()
		fmt.Println("Forwarding request to node", node)
		return nil, client, false, nil
	}
	ha.peerMutex.RUnlock()
	// else, if this is the best node

	ha.deviceMutex.Lock()
	if device, have = ha.devices[deviceId]; !have {
		device = &deviceData{
			loadingDone: make(chan struct{}),
		}
		ha.devices[deviceId] = device
		ha.deviceMutex.Unlock()
	} else {
		// some other thread loaded the device since we last checked
		ha.deviceMutex.Unlock()
		return ha.handlerFor(deviceId)
	}

	fmt.Println("Locking device", deviceId)
	// have the implementation lock & load the device
	if _, err := ha.implementation.Lock(context.Background(), &stateful.LockRequest{Device: deviceId}); err != nil {
		// failed to load the device, release it
		ha.deviceMutex.Lock()
		if dev, have := ha.devices[deviceId]; have && dev == device {
			delete(ha.devices, deviceId)
		}
		ha.deviceMutex.Unlock()

		//inform any waiting threads that loading failed
		device.loadingError = err
		close(device.loadingDone)
		return nil, nil, false, err
	} else {
		close(device.loadingDone)

		// after loading the device, restart routing, as things may have changed in the time it took to load
		return ha.handlerFor(deviceId)
	}
}

// stateful service pass-through (forward to appropriate node, or process locally)

func (ha *routingService) SetData(ctx context.Context, request *stateful.SetDataRequest) (*empty.Empty, error) {
	if device, remoteHandler, useLocal, err := ha.handlerFor(request.Device); err != nil {
		return &empty.Empty{}, err
	} else if useLocal {
		defer device.mutex.RUnlock()

		return ha.implementation.SetData(ctx, request)
	} else {
		return remoteHandler.SetData(ctx, request)
	}
}

func (ha *routingService) GetData(ctx context.Context, request *stateful.GetDataRequest) (*stateful.GetDataResponse, error) {
	if device, remoteHandler, useLocal, err := ha.handlerFor(request.Device); err != nil {
		return &stateful.GetDataResponse{}, err
	} else if useLocal {
		defer device.mutex.RUnlock()

		return ha.implementation.GetData(ctx, request)
	} else {
		return remoteHandler.GetData(ctx, request)
	}
}
