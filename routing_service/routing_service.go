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
	"net"
	"sync"
	"time"
)

const maxBackoff = time.Second * 4
const waitReadyTime = time.Second * 5

type routingService struct {
	ordinal uint32

	peerMutex sync.RWMutex
	ready     bool
	peers     map[uint32]*node

	deviceMutex sync.RWMutex
	devices     map[uint64]*deviceData

	implementation stateful.StatefulServer
}

type node struct {
	stateful.StatefulClient
	peer.PeerClient
	clientConn grpc.ClientConn
	connected  bool // have I connected to this node
	ready      bool // has this node informed me it's ready
}

type deviceData struct {
	mutex       sync.RWMutex
	doneLoading chan struct{}
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

	lis, err := net.Listen("tcp", addressFromOrdinal(ha.ordinal))
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
	//assume all nodes that are up have connected
	ha.peerMutex.Lock()
	defer ha.peerMutex.Unlock()

	fmt.Println("-- ready --")
	ha.ready = true

	// notify all nodes that we're up & ready
	for _, con := range ha.peers {
		if con.connected {
			go con.Ready(context.Background(), &peer.ReadyRequest{Ordinal: ha.ordinal})
		}
	}
}

func addressFromOrdinal(ordinal uint32) string {
	return fmt.Sprintf("localhost:%d", 6000+ordinal)
}

func (ha *routingService) connect(ordinal uint32) {
	ha.peerMutex.Lock()
	defer ha.peerMutex.Unlock()

	if _, have := ha.peers[ordinal]; !have {
		fmt.Println("connecting to", ordinal)
		cc, err := grpc.Dial(addressFromOrdinal(ordinal), grpc.WithInsecure(), grpc.WithBackoffConfig(grpc.BackoffConfig{MaxDelay: maxBackoff}))
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
				ready := ha.ready
				ha.peerMutex.RUnlock()
				if ready {
					peer.NewPeerClient(cc).Ready(context.Background(), &peer.ReadyRequest{Ordinal: ha.ordinal})
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

func (ha *routingService) Ready(ctx context.Context, request *peer.ReadyRequest) (*empty.Empty, error) {
	fmt.Println("Node", request.Ordinal, "declared ready")
	ha.peerMutex.Lock()
	ha.peers[request.Ordinal].ready = true
	ha.peerMutex.Unlock()

	ha.deviceMutex.Lock()
	defer ha.deviceMutex.Unlock()

	devicesToMove := make(map[uint64]*deviceData)
	for deviceId, device := range ha.devices {
		//for every device that belongs on the other node
		if BestNode(deviceId, ha.ordinal, map[uint32]struct{}{request.Ordinal: {}}) == request.Ordinal {
			fmt.Println("will migrate device", deviceId)
			//release and notify that it's moved
			devicesToMove[deviceId] = device
			delete(ha.devices, deviceId)
		}
	}

	go func() {
		for deviceId, device := range devicesToMove {
			// drain all in-progress requests
			device.mutex.Lock()
			fmt.Println("Unlocking device", deviceId)
			ha.implementation.Unlock(context.Background(), //always run to completion
				&stateful.UnlockRequest{Device: deviceId})

			go ha.Handoff(context.Background(), &peer.HandoffRequest{Device: deviceId})

			device.mutex.Unlock()
		}
	}()

	return &empty.Empty{}, nil
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
		<-device.doneLoading
		return device, nil, true, nil
	}
	ha.deviceMutex.RUnlock()

	// build a list of possible nodes, including this one
	possibleNodes := make(map[uint32]struct{})
	ha.peerMutex.RLock()
	if ha.ready {
		possibleNodes[ha.ordinal] = struct{}{}
	}
	for nodeId, node := range ha.peers {
		if node.connected && node.ready {
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
			doneLoading: make(chan struct{}),
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
		return nil, nil, false, err
	}
	close(device.doneLoading)

	// after loading the device (which may take some time), restart routing
	return ha.handlerFor(deviceId)
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
