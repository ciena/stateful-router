package stateful_service

import (
	"context"
	"github.com/golang/protobuf/ptypes/empty"
	"github.com/khagerma/stateful-experiment/protos/db"
	"github.com/khagerma/stateful-experiment/protos/server"
	"google.golang.org/grpc"
	"sync"
)

// stateful service implementation

type statefulService struct {
	dbClient db.DBClient

	mutex      sync.Mutex
	localState map[uint64][]byte
}

func New() stateful.StatefulServer {
	cc, err := grpc.Dial("localhost:2345", grpc.WithInsecure())
	if err != nil {
		panic(err)
	}
	ha := &statefulService{
		dbClient:   db.NewDBClient(cc),
		localState: make(map[uint64][]byte),
	}
	return ha
}

func (ha *statefulService) Lock(ctx context.Context, request *stateful.LockRequest) (*stateful.LockResponse, error) {
	// acquire lock
	response, err := ha.dbClient.Lock(ctx, &db.LockRequest{Device: request.Device})
	if err != nil {
		return &stateful.LockResponse{}, err
	}

	// load state from db
	dataResponse, err := ha.dbClient.GetData(ctx, &db.GetDataRequest{Lock: response.LockId, Device: request.Device})
	if err != nil {
		return &stateful.LockResponse{}, err
	}
	ha.mutex.Lock()
	ha.localState[request.Device] = dataResponse.Data
	ha.mutex.Unlock()

	return &stateful.LockResponse{LockId: response.LockId}, nil
}

func (ha *statefulService) Unlock(ctx context.Context, request *stateful.UnlockRequest) (*empty.Empty, error) {
	ha.mutex.Lock()
	data := ha.localState[request.Device]
	delete(ha.localState, request.Device)
	ha.mutex.Unlock()

	// flush data to db
	ha.dbClient.SetData(ctx, &db.SetDataRequest{Lock: request.LockId, Device: request.Device, Data: data})

	// release lock
	return ha.dbClient.Unlock(ctx, &db.UnlockRequest{Device: request.Device, LockId: request.LockId})
}

func (ha *statefulService) SetData(ctx context.Context, request *stateful.SetDataRequest) (*empty.Empty, error) {
	// process, update local or db state as needed
	ha.mutex.Lock()
	if _, have := ha.localState[request.Device]; !have {
		panic("device not loaded; this should not be possible")
	}

	ha.localState[request.Device] = request.Data
	ha.mutex.Unlock()

	return &empty.Empty{}, nil
}

func (ha *statefulService) GetData(ctx context.Context, request *stateful.GetDataRequest) (*stateful.GetDataResponse, error) {
	ha.mutex.Lock()
	defer ha.mutex.Unlock()

	data, have := ha.localState[request.Device]
	if !have {
		// for demonstration purposes only
		panic("device not loaded; this should not be possible")
	}

	return &stateful.GetDataResponse{Data: data}, nil
}
