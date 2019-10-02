package routing

import (
	"context"
	"errors"
	"fmt"
	"github.com/golang/protobuf/ptypes/empty"
	"github.com/khagerma/stateful-experiment/protos/server"
	"math"
	"math/rand"
	"sync"
	"testing"
	"time"
)

const assumePropagationDelay = time.Millisecond * 30
const assumeStabilizationDelay = time.Millisecond * 100

func init() {
	//since everything is local, assume connectivity is established within 1s
	waitReadyTime = time.Second
}

// the device w/ UUID 0 is a special case
func TestRoutingServiceDevice0(t *testing.T) {
	ss, clients := setup(3)
	defer teardown(clients)

	// create device w/ UUID 0
	randomClient := rand.Intn(len(clients))
	setDeviceData(t, clients, randomClient, 0, []byte(fmt.Sprintf("test string from %d", 0)))

	if err := ss.waitForMigrationToStabilize(t); err != nil {
		t.Error(err)
		return
	}

	if err := verifyDeviceCounts(clients, 1); err != nil {
		t.Error(err)
		return
	}
}

func TestRoutingServiceDeviceMAX(t *testing.T) {
	ss, clients := setup(5)
	defer teardown(clients)

	// create device w/ UUID 0
	randomClient := rand.Intn(len(clients))
	setDeviceData(t, clients, randomClient, math.MaxUint64, []byte(fmt.Sprintf("test string from 0x%x", uint64(math.MaxUint64))))

	if err := ss.waitForMigrationToStabilize(t); err != nil {
		t.Error(err)
		return
	}

	if err := verifyDeviceCounts(clients, 1); err != nil {
		t.Error(err)
		return
	}
}

func TestRoutingServiceSingleCore(t *testing.T) {
	totalDevices := 4
	ss, clients := setup(1)
	defer teardown(clients)

	for i := 1; i <= totalDevices; i++ {
		// create device
		randomClient := rand.Intn(len(clients))
		deviceId := uint64(rand.Uint32())<<32 | uint64(i)
		setDeviceData(t, clients, randomClient, deviceId, []byte(fmt.Sprintf("test string from %d", i)))

		if err := ss.waitForMigrationToStabilize(t); err != nil {
			t.Error(err)
			return
		}

		if err := verifyDeviceCounts(clients, i); err != nil {
			t.Error(err)
			return
		}
	}
}

func TestRoutingServiceSerializedAdd(t *testing.T) {
	totalDevices := 30
	totalNodes := 3
	ss, clients := setup(totalNodes)
	defer teardown(clients)

	for i := 1; i <= totalDevices; i++ {
		// create device
		randomClient := rand.Intn(len(clients))
		deviceId := uint64(rand.Uint32())<<32 | uint64(i)
		setDeviceData(t, clients, randomClient, deviceId, []byte(fmt.Sprintf("test string from %d", i)))

		if err := ss.waitForMigrationToStabilize(t); err != nil {
			t.Error(err)
			return
		}

		if err := verifyDeviceCounts(clients, i); err != nil {
			t.Error(err)
			return
		}
	}
}

func TestRoutingServiceParallelAdd(t *testing.T) {
	totalDevices := 100
	totalNodes := 3
	ss, clients := setup(totalNodes)
	defer teardown(clients)

	// create devices
	for i := 0; i < totalDevices; i++ {
		randomClient := rand.Intn(len(clients))
		deviceId := uint64(rand.Uint32())<<32 | uint64(i)
		go setDeviceData(t, clients, randomClient, deviceId, []byte(fmt.Sprintf("test string from %d", i)))
	}

	if err := ss.waitForMigrationToStabilize(t); err != nil {
		t.Error(err)
		return
	}

	if err := verifyDeviceCounts(clients, totalDevices); err != nil {
		t.Error(err)
		return
	}
}

func TestRoutingServiceManyNodes(t *testing.T) {
	totalDevices := 100
	totalNodes := 7
	ss, clients := setup(totalNodes)
	defer teardown(clients)

	for i := 0; i < totalDevices; {
		// create devices in groups of 10
		for j := 0; j < 10; j++ {
			randomClient := rand.Intn(len(clients))
			deviceId := uint64(rand.Uint32())<<32 | uint64(i)
			setDeviceData(t, clients, randomClient, deviceId, []byte(fmt.Sprintf("test string from %d", i)))
			i++
		}

		if err := ss.waitForMigrationToStabilize(t); err != nil {
			t.Error(err)
			return
		}

		if err := verifyDeviceCounts(clients, i); err != nil {
			t.Error(err)
			return
		}
	}
}

func TestRoutingServiceAddNodes(t *testing.T) {
	totalDevices := 100
	finalNodes := 7
	ss, clients := setup(1)
	defer teardown(clients)

	// create devices
	for i := 0; i < totalDevices; i++ {
		randomClient := rand.Intn(len(clients))
		deviceId := uint64(rand.Uint32())<<32 | uint64(i)
		setDeviceData(t, clients, randomClient, deviceId, []byte(fmt.Sprintf("test string from %d", i)))
	}

	for len(clients) < finalNodes {
		// add a client
		clients = append(clients, NewRoutingService(fmt.Sprintf("localhost:5%03d", len(clients)), "localhost:5%03d", uint32(len(clients)), &dummyStatefulServerProxy{ss: ss}).(*routingService))

		time.Sleep(waitReadyTime + assumePropagationDelay)

		if err := ss.waitForMigrationToStabilize(t); err != nil {
			t.Error(err)
			return
		}

		if err := verifyDeviceCounts(clients, totalDevices); err != nil {
			t.Error(err)
			return
		}
	}
}

func setup(nodes int) (*dummyStatefulServer, []*routingService) {
	// fake local DB
	ss := &dummyStatefulServer{deviceData: make(map[uint64][]byte), deviceOwner: make(map[uint64]uint32)}

	// setup clients
	clients := make([]*routingService, nodes)
	for i := range clients {
		clients[i] = NewRoutingService(fmt.Sprintf("localhost:5%03d", i), "localhost:5%03d", uint32(i), &dummyStatefulServerProxy{ss: ss}).(*routingService)
	}

	// wait for clients to connect to each other
	time.Sleep(waitReadyTime + time.Millisecond*500)

	return ss, clients
}

func teardown(clients []*routingService) {
	for _, client := range clients {
		client.Stop()
	}
}

func setDeviceData(t *testing.T, clients []*routingService, client int, device uint64, data []byte) {
	if _, err := clients[client].SetData(context.Background(), &stateful.SetDataRequest{
		Device: device,
		Data:   data,
	}); err != nil {
		t.Error(err)
	}
}

func (ss *dummyStatefulServer) waitForMigrationToStabilize(t *testing.T) error {
	// wait a moment (give time for at least one request to run all the way through)
	time.Sleep(assumePropagationDelay)
	now := time.Now()
	// repeat as long as a request has just recently occurred
	for startTime, lastRequestTime := now, now; now.Before(lastRequestTime.Add(assumeStabilizationDelay)); now = time.Now() {
		if now.After(startTime.Add(time.Second * 2)) {
			return errors.New("output failed to stabilize")
		}
		// requests haven't yet stabilized
		time.Sleep(lastRequestTime.Add(assumeStabilizationDelay).Sub(now))

		ss.mutex.Lock()
		lastRequestTime = ss.lastRequestTime
		ss.mutex.Unlock()
	}
	return nil
}

func verifyDeviceCounts(clients []*routingService, numDevices int) error {
	for i, client := range clients {
		spare := 0
		if i < numDevices%len(clients) {
			spare = 1
		}
		shouldHave, have := numDevices/len(clients)+spare, len(client.devices)
		if have != shouldHave {
			str := fmt.Sprintf("wrong number of devices on core (%d total)\n", numDevices)
			for i, client := range clients {
				str += fmt.Sprintln(i, "has", len(client.devices), "devices")
			}
			str += fmt.Sprintf("client %d should have %d devices, has %d\n", i, shouldHave, have)
			return errors.New(str)
		}
	}
	return nil
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
		panic("device should be owned when Unlock() is called")
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
		panic("device should be owned when SetData() is called")
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
		panic("device should be owned when GetData() is called")
	}
	if ss.fail {
		return &stateful.GetDataResponse{}, errors.New("test error")
	}
	return &stateful.GetDataResponse{Data: ss.deviceData[r.Device]}, nil
}
