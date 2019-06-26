package ha_service

import (
	"context"
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

type routingService struct {
	mutex sync.Mutex

	ordinal uint32

	ready bool

	connections         map[uint32]stateful.StatefulClient
	peerConnectionReady map[uint32]struct{}
	peerReady           map[uint32]struct{}

	deviceLock             sync.RWMutex
	lockedAndLoadedDevices map[uint64]uint64

	implementation stateful.StatefulServer
}

func NewRoutingService(ordinal uint32) stateful.StatefulServer {
	ss := &statefulService{}
	ss.start()

	ha := &routingService{
		ordinal:                ordinal,
		connections:            make(map[uint32]stateful.StatefulClient),
		peerConnectionReady:    make(map[uint32]struct{}),
		peerReady:              make(map[uint32]struct{}),
		lockedAndLoadedDevices: make(map[uint64]uint64),
		implementation:         ss,
	}

	go ha.start()
	return ha
}

func (ha *routingService) start() {
	lis, err := net.Listen("tcp", addressFromOrdinal(ha.ordinal))
	if err != nil {
		panic(fmt.Sprintf("failed to listen: %v", err))
	}

	for i := uint32(0); i < ha.ordinal; i++ {
		ha.connect(i)
	}

	s := grpc.NewServer(grpc.KeepaliveParams(keepalive.ServerParameters{Time: time.Second * 2, Timeout: time.Second * 2}))
	stateful.RegisterStatefulServer(s, ha)
	peer.RegisterPeerServer(s, ha)
	if err := s.Serve(lis); err != nil {
		panic(fmt.Sprintf("failed to serve: %v", err))
	}
}

func addressFromOrdinal(ordinal uint32) string {
	return fmt.Sprintf("localhost:%d", 6000+ordinal)
}

func (ha *routingService) connect(ordinal uint32) {
	ha.mutex.Lock()
	defer ha.mutex.Unlock()

	if _, have := ha.connections[ordinal]; !have {
		fmt.Println("connecting to", ordinal)
		cc, err := grpc.Dial(addressFromOrdinal(ordinal), grpc.WithInsecure())
		if err != nil {
			panic(err)
		}

		go ha.watchState(cc, ordinal)

		ha.connections[ordinal] = stateful.NewStatefulClient(cc)
	}
}

func (ha *routingService) watchState(cc *grpc.ClientConn, ordinal uint32) {
	state := connectivity.Connecting
	for cc.WaitForStateChange(context.Background(), state) {
		lastState := state
		state = cc.GetState()

		if state == connectivity.Ready {
			ha.mutex.Lock()
			ha.peerConnectionReady[ordinal] = struct{}{}
			ha.mutex.Unlock()
			ha.loadDevices(peer.NewPeerClient(cc))
		} else if lastState == connectivity.Ready {
			ha.mutex.Lock()
			delete(ha.peerConnectionReady, ordinal)

			if ordinal > ha.ordinal {
				delete(ha.connections, ordinal)
				ha.mutex.Unlock()
				break
			}

			ha.mutex.Unlock()

		}
	}
	cc.Close()
}

func (ha *routingService) loadDevices(client peer.PeerClient) {
	response, err := client.Connected(context.Background(), &peer.ConnectedRequest{Ordinal: ha.ordinal})
	fmt.Println("Connected response:", response.Devices, err)
}

// -- implementations --

// peer interface impl

func (ha *routingService) Connected(ctx context.Context, request *peer.ConnectedRequest) (*peer.ConnectedResponse, error) {
	fmt.Println("Connected:", request.Ordinal)
	ha.connect(request.Ordinal)

	ha.deviceLock.RLock()
	var devices []uint64
	for id := range ha.lockedAndLoadedDevices {
		// return all nodes that are currently on this node, where the other node would be preferred
		if BestNode(id, ha.ordinal, map[uint32]struct{}{request.Ordinal: {}}) == request.Ordinal {
			devices = append(devices, id)
		}
	}
	ha.deviceLock.RUnlock()
	fmt.Println("ret:", devices)
	return &peer.ConnectedResponse{Devices: devices}, nil
}

func (ha *routingService) Ready(ctx context.Context, request *peer.ReadyRequest) (*empty.Empty, error) {
	ha.mutex.Lock()
	ha.peerReady[request.Ordinal] = struct{}{}
	ha.mutex.Unlock()
	return &empty.Empty{}, nil
}

func (ha *routingService) Handoff(ctx context.Context, request *peer.HandoffRequest) (*empty.Empty, error) {
	ha.deviceLock.Lock()
	lock, have := ha.lockedAndLoadedDevices[request.Device]
	ha.deviceLock.Unlock()

	if have {
		return ha.implementation.Unlock(context.Background(), &stateful.UnlockRequest{Device: request.Device, LockId: lock})
	} else {
		// we no longer have the lock for this device
		return &empty.Empty{}, nil
	}
}

// stateful service interface impl

func (ha *routingService) Lock(ctx context.Context, request *stateful.LockRequest) (*stateful.LockResponse, error) {
	panic("not implemented")
}
func (ha *routingService) lock(device uint64) uint64 {
	ha.deviceLock.Lock()
	defer ha.deviceLock.Unlock()

	response, err := ha.implementation.Lock(context.Background(), &stateful.LockRequest{Device: device})
	if err != nil {
		panic(err)
	}
	ha.lockedAndLoadedDevices[device] = response.LockId
	return response.LockId
}

func (ha *routingService) Unlock(_ context.Context, request *stateful.UnlockRequest) (*empty.Empty, error) {
	panic("not implemented")
}
func (ha *routingService) unlock(device, lock uint64) error {
	ha.deviceLock.Lock()
	defer ha.deviceLock.Unlock()

	_, err := ha.implementation.Unlock(context.Background() /*always run to completion*/, &stateful.UnlockRequest{Device: device, LockId: lock})
	if err == nil {
		delete(ha.lockedAndLoadedDevices, device)
	}
	return err
}

// handlerFor returns a processor for the given device
// the processor either handles the request locally,
// or forwards it on to the appropriate peer
func (ha *routingService) handlerFor(device uint64) (uint64, stateful.StatefulServer, stateful.StatefulClient, bool) {
	ha.deviceLock.RLock()
	lock, have := ha.lockedAndLoadedDevices[device]
	ha.deviceLock.RUnlock()

	if !have {
		node := BestNode(device, ha.ordinal, ha.peerReady)
		fmt.Println("here", node)
		if node != ha.ordinal {
			return lock, nil, ha.connections[node], false
		}
		lock = ha.lock(device)
	}
	return lock, ha.implementation, nil, true
}

// stateful service pass-through (forward to appropriate node, or process locally)

func (ha *routingService) SetData(ctx context.Context, request *stateful.SetDataRequest) (*empty.Empty, error) {
	if lock, localHandler, remoteHandler, useLocal := ha.handlerFor(request.Device); useLocal {
		request.Lock = lock
		return localHandler.SetData(ctx, request)
	} else {
		return remoteHandler.SetData(ctx, request)
	}
}

func (ha *routingService) GetData(ctx context.Context, request *stateful.GetDataRequest) (*stateful.GetDataResponse, error) {
	if lock, localHandler, remoteHandler, useLocal := ha.handlerFor(request.Device); useLocal {
		request.Lock = lock
		return localHandler.GetData(ctx, request)
	} else {
		return remoteHandler.GetData(ctx, request)
	}
}
