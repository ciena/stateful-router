package ha_service

import (
	"context"
	"github.com/golang/protobuf/ptypes/empty"
	"github.com/khagerma/stateful-experiment/protos/db"
	"github.com/khagerma/stateful-experiment/protos/server"
	"google.golang.org/grpc"
)

// stateful service implementation

type statefulService struct {
	dbClient db.DBClient
}

func (ha *statefulService) start() {
	cc, err := grpc.Dial("localhost:2345", grpc.WithInsecure())
	if err != nil {
		panic(err)
	}
	ha.dbClient = db.NewDBClient(cc)
}

func (ha *statefulService) Lock(ctx context.Context, request *stateful.LockRequest) (*stateful.LockResponse, error) {
	// process, load local state, whatever.
	response, err := ha.dbClient.Lock(context.Background(), &db.LockRequest{Device: request.Device})
	return &stateful.LockResponse{LockId: response.LockId}, err
}

func (ha *statefulService) Unlock(ctx context.Context, request *stateful.UnlockRequest) (*empty.Empty, error) {
	//flush data to db, release lock
	return ha.dbClient.Unlock(ctx, &db.UnlockRequest{Device: request.Device, LockId: request.LockId})
}

func (ha *statefulService) SetData(ctx context.Context, request *stateful.SetDataRequest) (*empty.Empty, error) {
	// process, update local or db state as needed
	return ha.dbClient.SetData(ctx, &db.SetDataRequest{Lock: request.Lock, Device: request.Device, Data: request.Data})
}

func (ha *statefulService) GetData(ctx context.Context, request *stateful.GetDataRequest) (*stateful.GetDataResponse, error) {
	response, err := ha.dbClient.GetData(ctx, &db.GetDataRequest{Lock: request.Lock, Device: request.Device})
	return &stateful.GetDataResponse{Data: response.Data}, err
}

