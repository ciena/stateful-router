package stateful_service

import (
	"context"
	"github.com/golang/protobuf/ptypes/empty"
	"github.com/khagerma/stateful-experiment/protos/db"
	"github.com/khagerma/stateful-experiment/protos/server"
	"google.golang.org/grpc"
	"os"
	"sync"
	"time"
)

// stateful service implementation

type statefulService struct {
	dbClient db.DBClient

	mutex      sync.Mutex
	localState map[uint64]*state
}

type state struct {
	lock uint64
	data []byte
}

func New() stateful.StatefulServer {
	addr, have:=os.LookupEnv("DUMMY_DB_ADDRESS")
	if !have{
		panic("env var DUMMY_DB_ADDRESS not set")
	}

	cc, err := grpc.Dial(addr, grpc.WithInsecure())
	if err != nil {
		panic(err)
	}
	ha := &statefulService{
		dbClient:   db.NewDBClient(cc),
		localState: make(map[uint64]*state),
	}
	return ha
}

func (ha *statefulService) Lock(ctx context.Context, request *stateful.LockRequest) (*empty.Empty, error) {
	// acquire lock
	response, err := ha.dbClient.Lock(ctx, &db.LockRequest{Device: request.Device})
	if err != nil {
		return &empty.Empty{}, err
	}

	// load state from db
	dataResponse, err := ha.dbClient.GetData(ctx, &db.GetDataRequest{Lock: response.LockId, Device: request.Device})
	if err != nil {
		return &empty.Empty{}, err
	}
	ha.mutex.Lock()
	ha.localState[request.Device] = &state{lock: response.LockId, data: dataResponse.Data}
	ha.mutex.Unlock()

	// demonstrate time delay
	time.Sleep(time.Second)

	return &empty.Empty{}, nil
}

func (ha *statefulService) Unlock(ctx context.Context, request *stateful.UnlockRequest) (*empty.Empty, error) {
	ha.mutex.Lock()
	state := ha.localState[request.Device]
	delete(ha.localState, request.Device)
	ha.mutex.Unlock()

	// demonstrate time delay
	time.Sleep(time.Second)

	// flush data to db
	ha.dbClient.SetData(ctx, &db.SetDataRequest{Device: request.Device, Lock: state.lock, Data: state.data})

	// release lock
	return ha.dbClient.Unlock(ctx, &db.UnlockRequest{Device: request.Device, LockId: state.lock})
}

func (ha *statefulService) SetData(ctx context.Context, request *stateful.SetDataRequest) (*empty.Empty, error) {
	// process, update local or db state as needed
	ha.mutex.Lock()
	if _, have := ha.localState[request.Device]; !have {
		panic("device not loaded; this should not be possible") // for demonstration purposes only
	}

	ha.localState[request.Device].data = request.Data
	ha.mutex.Unlock()

	return &empty.Empty{}, nil
}

func (ha *statefulService) GetData(ctx context.Context, request *stateful.GetDataRequest) (*stateful.GetDataResponse, error) {
	ha.mutex.Lock()
	defer ha.mutex.Unlock()

	state, have := ha.localState[request.Device]
	if !have {
		panic("device not loaded; this should not be possible") // for demonstration purposes only
	}

	return &stateful.GetDataResponse{Data: state.data}, nil
}
