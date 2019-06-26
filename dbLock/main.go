package main

import (
	"context"
	"errors"
	"fmt"
	"github.com/golang/protobuf/ptypes/empty"
	"github.com/khagerma/stateful-experiment/protos/db"
	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"
	"net"
	"sync"
	"time"
)

var maxTime = time.Unix(1<<63-62135596801, 999999999)

func main() {
	lis, err := net.Listen("tcp", "localhost:2345")
	if err != nil {
		panic(fmt.Sprintf("failed to listen: %v", err))
	}
	s := grpc.NewServer(grpc.KeepaliveParams(keepalive.ServerParameters{Time: time.Second * 2, Timeout: time.Second * 2}))
	db.RegisterDBServer(s, &dbConnection{
		devices: make(map[uint64]*deviceData),
	})
	fmt.Println("Listening on localhost:2345...")
	if err := s.Serve(lis); err != nil {
		panic(fmt.Sprintf("failed to serve: %v", err))
	}
}

type dbConnection struct {
	mutex     sync.Mutex
	lockIDCtr uint64
	devices   map[uint64]*deviceData
}

type deviceData struct {
	mutex  chan struct{}
	expire time.Time
	lockID uint64

	data []byte
}

func (i *dbConnection) nextLockID() uint64 {
	if i.lockIDCtr == 0 {
		i.lockIDCtr++
	}
	ret := i.lockIDCtr
	i.lockIDCtr++
	return ret
}

func (i dbConnection) getDevice(id uint64) *deviceData {
	i.mutex.Lock()
	defer i.mutex.Unlock()

	data, have := i.devices[id]
	if !have {
		data = &deviceData{mutex: make(chan struct{}, 1)}
		i.devices[id] = data
	}
	return data
}

func (i dbConnection) Lock(ctx context.Context, request *db.LockRequest) (*db.LockResponse, error) {
	fmt.Println("start Lock()")
	defer fmt.Println("end Lock()")

	device := i.getDevice(request.Device)
	select {
	case <-ctx.Done():
		// disconnected before device lock acquired
		return &db.LockResponse{}, nil

	case device.mutex <- struct{}{}: //acquire lock
		i.mutex.Lock()
		defer i.mutex.Unlock()

		lockId := i.nextLockID()
		device.lockID = lockId
		device.expire = time.Now().Add(time.Second * 5)
		return &db.LockResponse{LockId: lockId}, nil
	}
}

func (i dbConnection) Unlock(ctx context.Context, request *db.UnlockRequest) (*empty.Empty, error) {
	fmt.Println("start Unlock()")
	defer fmt.Println("end Unlock()")

	device := i.getDevice(request.Device)

	i.mutex.Lock()
	defer i.mutex.Unlock()

	if device.lockID == request.LockId {
		device.lockID = 0
		device.expire = maxTime
		<-device.mutex
	}
	return &empty.Empty{}, nil
}

func (i dbConnection) SetData(ctx context.Context, request *db.SetDataRequest) (*empty.Empty, error) {
	fmt.Println("start SetData()")
	defer fmt.Println("end SetData()")

	device := i.getDevice(request.Device)

	i.mutex.Lock()
	defer i.mutex.Unlock()

	if device.lockID == request.Lock {
		device.data = request.Data
		return &empty.Empty{}, nil
	} else {
		return &empty.Empty{}, errors.New("the device lock has been acquired by another process")
	}
}

func (i dbConnection) GetData(ctx context.Context, request *db.GetDataRequest) (*db.GetDataResponse, error) {
	fmt.Println("start GetData()")
	defer fmt.Println("end GetData()")
	device := i.getDevice(request.Device)

	i.mutex.Lock()
	defer i.mutex.Unlock()

	if device.lockID == request.Lock {
		return &db.GetDataResponse{Data: device.data}, nil
	} else {
		return &db.GetDataResponse{}, errors.New("the device lock has been acquired by another process")
	}
}
