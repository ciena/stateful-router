package routing

import (
	"context"
	"errors"
	"fmt"
	"github.com/golang/protobuf/ptypes/empty"
	"github.com/khagerma/stateful-experiment/protos/server"
	"math/rand"
	"sync"
	"testing"
	"time"
)

func TestRoutingService(t *testing.T) {
	totalDevices := 100
	totalNodes := 3

	// fake local DB
	ss := &dummyStatefulServer{deviceData: make(map[uint64][]byte), deviceOwner: make(map[uint64]uint32)}

	// setup clients
	clients := make([]*routingService, totalNodes)
	for i := range clients {
		ss := &dummyStatefulServerProxy{ss: ss}
		clients[i] = NewRoutingService(fmt.Sprintf("localhost:5%03d", i), "localhost:5%03d", uint32(i), ss).(*routingService)
	}

	// wait for clients to connect to each other
	time.Sleep(waitReadyTime + time.Second)

	devicesSoFar := 0
	for i := 0; i < totalDevices; i++ {
		// create device
		randomClient := rand.Intn(len(clients))
		if _, err := clients[randomClient].SetData(context.Background(), &stateful.SetDataRequest{
			Device: uint64(rand.Uint32()) | uint64(i)<<32,
			Data:   []byte(fmt.Sprintf("test string from %d", i)),
		}); err != nil {
			t.Error(err)
		}
		devicesSoFar++

		// wait for device counts to stabilize
		time.Sleep(time.Millisecond * 1000)
		now := time.Now()
		for startTime, lastRequestTime := now, now; now.Before(lastRequestTime.Add(time.Millisecond * 30)); now = time.Now() {
			if now.After(startTime.Add(time.Second * 1)) {
				t.Error("output failed to stabilize")
				return
			}
			time.Sleep(lastRequestTime.Add(time.Millisecond * 30).Sub(now))

			ss.mutex.Lock()
			lastRequestTime = ss.lastRequestTime
			ss.mutex.Unlock()
		}

		for i, client := range clients {
			fmt.Println(i, "has", len(client.devices), "devices")

			spare := 0
			if i < devicesSoFar%len(clients) {
				spare = 1
			}
			shouldHave, have := devicesSoFar/len(clients)+spare, len(client.devices)
			if have != shouldHave {
				t.Errorf("failed on device %d", devicesSoFar)
				t.Errorf("client %d should have %d devices, has %d", i, shouldHave, have)
				return
			}
		}
	}
}

type dummyStatefulServerProxy struct {
	ordinal uint32
	ss      *dummyStatefulServer
}

func (ss *dummyStatefulServerProxy) Lock(ctx context.Context, r *stateful.LockRequest) (*empty.Empty, error) {
	return ss.ss.Lock(ctx, r, ss.ordinal)
}
func (ss *dummyStatefulServerProxy) Unlock(ctx context.Context, r *stateful.UnlockRequest) (*empty.Empty, error) {
	return ss.ss.Unlock(ctx, r, ss.ordinal)
}
func (ss *dummyStatefulServerProxy) SetData(ctx context.Context, r *stateful.SetDataRequest) (*empty.Empty, error) {
	return ss.ss.SetData(ctx, r, ss.ordinal)
}
func (ss *dummyStatefulServerProxy) GetData(ctx context.Context, r *stateful.GetDataRequest) (*stateful.GetDataResponse, error) {
	return ss.ss.GetData(ctx, r, ss.ordinal)
}

type dummyStatefulServer struct {
	mutex sync.Mutex

	lockCount    uint32
	unlockCount  uint32
	setDataCount uint32
	getDataCount uint32

	fail bool

	deviceData  map[uint64][]byte
	deviceOwner map[uint64]uint32

	lastRequestTime time.Time
}

func (ss *dummyStatefulServer) Lock(ctx context.Context, r *stateful.LockRequest, ordinal uint32) (*empty.Empty, error) {
	ss.mutex.Lock()
	defer ss.mutex.Unlock()

	ss.lastRequestTime = time.Now()
	ss.lockCount++

	if _, have := ss.deviceOwner[r.Device]; have {
		panic("device shouldn't be owned when Lock() is called")
	}
	ss.deviceOwner[r.Device] = ordinal

	if ss.fail {
		return &empty.Empty{}, errors.New("test error")
	}
	return &empty.Empty{}, nil
}
func (ss *dummyStatefulServer) Unlock(ctx context.Context, r *stateful.UnlockRequest, ordinal uint32) (*empty.Empty, error) {
	ss.mutex.Lock()
	defer ss.mutex.Unlock()

	ss.lastRequestTime = time.Now()
	ss.unlockCount++

	if owner, have := ss.deviceOwner[r.Device]; !have || owner != ordinal {
		panic("device shouldn't be owned when Lock() is called")
	}
	delete(ss.deviceOwner, r.Device)

	if ss.fail {
		return &empty.Empty{}, errors.New("test error")
	}
	return &empty.Empty{}, nil
}
func (ss *dummyStatefulServer) SetData(ctx context.Context, r *stateful.SetDataRequest, ordinal uint32) (*empty.Empty, error) {
	ss.mutex.Lock()
	defer ss.mutex.Unlock()

	ss.lastRequestTime = time.Now()
	ss.setDataCount++
	if owner, have := ss.deviceOwner[r.Device]; !have || owner != ordinal {
		panic("device shouldn't be owned when Lock() is called")
	}
	if ss.fail {
		return &empty.Empty{}, errors.New("test error")
	}
	ss.deviceData[r.Device] = r.Data
	return &empty.Empty{}, nil
}
func (ss *dummyStatefulServer) GetData(ctx context.Context, r *stateful.GetDataRequest, ordinal uint32) (*stateful.GetDataResponse, error) {
	ss.mutex.Lock()
	defer ss.mutex.Unlock()

	ss.lastRequestTime = time.Now()
	ss.getDataCount++
	if owner, have := ss.deviceOwner[r.Device]; !have || owner != ordinal {
		panic("device shouldn't be owned when Lock() is called")
	}
	if ss.fail {
		return &stateful.GetDataResponse{}, errors.New("test error")
	}
	return &stateful.GetDataResponse{Data: ss.deviceData[r.Device]}, nil
}
