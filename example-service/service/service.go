package service

import (
	"context"
	"fmt"
	"github.com/golang/protobuf/ptypes/empty"
	"github.com/kent-h/stateful-router"
	"github.com/kent-h/stateful-router/example-service/protos/db"
	"github.com/kent-h/stateful-router/example-service/protos/server"
	"google.golang.org/grpc"
	"net"
	"os"
	"sync"
)

const resourceTypeDevice = 0

type StatefulService struct {
	router   *router.Router
	dbClient db.DBClient
	server   *grpc.Server

	mutex      sync.Mutex
	localState map[string]*state
}

type state struct {
	lock uint64
	data []byte
}

func New(ordinal uint32, peerDNSFormat, address string) *StatefulService {
	addr, have := os.LookupEnv("DUMMY_DB_ADDRESS")
	if !have {
		panic("env var DUMMY_DB_ADDRESS not set")
	}

	cc, err := grpc.Dial(addr, grpc.WithInsecure())
	if err != nil {
		panic(err)
	}

	ha := &StatefulService{
		dbClient:   db.NewDBClient(cc),
		localState: make(map[string]*state),
	}

	go ha.start(ordinal, peerDNSFormat, address)
	return ha
}

func (ss *StatefulService) start(ordinal uint32, peerDNSFormat, address string) {
	// create routing instance
	ss.server = grpc.NewServer(router.GRPCSettings()...)
	ss.router = router.New(ss.server, ordinal, peerDNSFormat, ss, nil, resourceTypeDevice)
	// register self
	stateful.RegisterStatefulServer(ss.server, ss)
	// listen for requests
	listener, err := net.Listen("tcp", address)
	if err != nil {
		panic(fmt.Sprintf("failed to listen: %v", err))
	}
	if err := ss.server.Serve(listener); err != nil {
		panic(fmt.Sprintf("failed to serve: %v", err))
	}
}

func (ss *StatefulService) Stop() {
	ss.router.Stop()
	ss.server.Stop()
}

// implementing routing.DeviceLocker
func (ss *StatefulService) Load(ctx context.Context, _ router.ResourceType, device string) error {
	// acquire lock
	response, err := ss.dbClient.Lock(ctx, &db.LockRequest{Device: device})
	if err != nil {
		return err
	}

	// load state from db
	dataResponse, err := ss.dbClient.GetData(ctx, &db.GetDataRequest{Lock: response.LockId, Device: device})
	if err != nil {
		return err
	}
	ss.mutex.Lock()
	ss.localState[device] = &state{lock: response.LockId, data: dataResponse.Data}
	ss.mutex.Unlock()

	return nil
}

// implementing routing.DeviceLocker
func (ss *StatefulService) Unload(_ router.ResourceType, device string) {
	ss.mutex.Lock()
	state := ss.localState[device]
	delete(ss.localState, device)
	ss.mutex.Unlock()

	// flush data to db
	_, _ = ss.dbClient.SetData(context.Background(), &db.SetDataRequest{Device: device, Lock: state.lock, Data: state.data})

	// release lock
	_, _ = ss.dbClient.Unlock(context.Background(), &db.UnlockRequest{Device: device, LockId: state.lock})
}

func (ss *StatefulService) SetData(ctx context.Context, request *stateful.SetDataRequest) (*empty.Empty, error) {
	if mutex, cc, forward, err := ss.router.Locate(resourceTypeDevice, request.Device); err != nil {
		return &empty.Empty{}, err
	} else if forward {
		return stateful.NewStatefulClient(cc).SetData(ctx, request)
	} else {
		defer mutex.RUnlock()
	}

	// process, update local or db state as needed
	ss.mutex.Lock()
	defer ss.mutex.Unlock()

	if _, have := ss.localState[request.Device]; !have {
		panic("device not loaded; this should not be possible") // for demonstration purposes only
	}

	ss.localState[request.Device].data = request.Data

	return &empty.Empty{}, nil
}

func (ss *StatefulService) GetData(ctx context.Context, request *stateful.GetDataRequest) (*stateful.GetDataResponse, error) {
	if mutex, cc, forward, err := ss.router.Locate(resourceTypeDevice, request.Device); err != nil {
		return &stateful.GetDataResponse{}, err
	} else if forward {
		return stateful.NewStatefulClient(cc).GetData(ctx, request)
	} else {
		defer mutex.RUnlock()
	}

	ss.mutex.Lock()
	defer ss.mutex.Unlock()

	state, have := ss.localState[request.Device]
	if !have {
		panic("device not loaded; this should not be possible") // for demonstration purposes only
	}

	return &stateful.GetDataResponse{Data: state.data}, nil
}
