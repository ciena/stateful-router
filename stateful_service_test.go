package router

import (
	"context"
	"errors"
	"fmt"
	"github.com/golang/protobuf/ptypes/empty"
	"github.com/kent-h/stateful-router/protos/test"
	"google.golang.org/grpc"
	"math"
	"math/rand"
	"net"
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
	sendTestRequest(t, clients, randomClient, 0)

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

	// create device w/ UUID MAX
	randomClient := rand.Intn(len(clients))
	sendTestRequest(t, clients, randomClient, math.MaxUint64)

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
		sendTestRequest(t, clients, randomClient, deviceId)

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
		sendTestRequest(t, clients, randomClient, deviceId)

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
		go sendTestRequest(t, clients, randomClient, deviceId)
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
			sendTestRequest(t, clients, randomClient, deviceId)
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
		sendTestRequest(t, clients, randomClient, deviceId)
	}

	for len(clients) < finalNodes {
		// add a client
		client := &dummyStatefulServerProxy{ss: ss}
		go client.start(uint32(len(clients)), "localhost:5%03d", fmt.Sprintf("localhost:5%03d", len(clients)))
		clients = append(clients, client)

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

func setup(nodes int) (*dummyStatefulServer, []*dummyStatefulServerProxy) {
	// fake local DB
	ss := &dummyStatefulServer{deviceOwner: make(map[uint64]uint32)}

	// setup clients
	clients := make([]*dummyStatefulServerProxy, nodes)
	for i := range clients {
		client := &dummyStatefulServerProxy{ss: ss}
		go client.start(uint32(i), "localhost:5%03d", fmt.Sprintf("localhost:5%03d", i))
		clients[i] = client
	}

	// wait for clients to connect to each other
	time.Sleep(waitReadyTime + time.Millisecond*500)

	return ss, clients
}

func teardown(clients []*dummyStatefulServerProxy) {
	for _, client := range clients {
		client.stop()
	}
}

func sendTestRequest(t *testing.T, clients []*dummyStatefulServerProxy, client int, device uint64) {
	if _, err := clients[client].Test(context.Background(), &test.TestRequest{
		Device: device,
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

func verifyDeviceCounts(clients []*dummyStatefulServerProxy, numDevices int) error {
	for i, client := range clients {
		spare := 0
		if i < numDevices%len(clients) {
			spare = 1
		}
		shouldHave, have := numDevices/len(clients)+spare, len(client.router.devices)
		if have != shouldHave {
			str := fmt.Sprintf("wrong number of devices on core (%d total)\n", numDevices)
			for i, client := range clients {
				str += fmt.Sprintln(i, "has", len(client.router.devices), "devices")
			}
			str += fmt.Sprintf("client %d should have %d devices, has %d\n", i, shouldHave, have)
			return errors.New(str)
		}
	}
	return nil
}

type dummyStatefulServerProxy struct {
	ordinal uint32
	router  *Router
	server  *grpc.Server
	ss      *dummyStatefulServer
}

func (ss *dummyStatefulServerProxy) start(ordinal uint32, peerDNSFormat, address string) {
	// create routing instance
	ss.server = grpc.NewServer(GRPCSettings()...)
	ss.router = New(ss.server, ordinal, peerDNSFormat, ss, nil)
	// register self
	test.RegisterTestServer(ss.server, ss)
	// listen for requests
	listener, err := net.Listen("tcp", address)
	if err != nil {
		panic(fmt.Sprintf("failed to listen: %v", err))
	}
	if err := ss.server.Serve(listener); err != nil {
		panic(fmt.Sprintf("failed to serve: %v", err))
	}
}

func (ss *dummyStatefulServerProxy) stop() {
	ss.router.Stop()
	ss.server.Stop()
}

func (ss *dummyStatefulServerProxy) Load(ctx context.Context, device uint64) error {
	return ss.ss.Load(ctx, device, ss.ordinal)
}
func (ss *dummyStatefulServerProxy) Unload(device uint64) {
	ss.ss.Unload(device, ss.ordinal)
}
func (ss *dummyStatefulServerProxy) Test(ctx context.Context, r *test.TestRequest) (*empty.Empty, error) {
	if mutex, cc, forward, err := ss.router.Locate(r.Device); err != nil {
		return &empty.Empty{}, err
	} else if forward {
		return test.NewTestClient(cc).Test(ctx, r)
	} else {
		defer mutex.RUnlock()
	}

	return ss.ss.Test(ctx, r, ss.ordinal)
}

type dummyStatefulServer struct {
	mutex sync.Mutex

	lockCount    uint32
	unlockCount  uint32
	requestCount uint32

	fail bool

	deviceOwner map[uint64]uint32

	lastRequestTime time.Time
}

func (ss *dummyStatefulServer) Load(ctx context.Context, device uint64, ordinal uint32) error {
	ss.mutex.Lock()
	defer ss.mutex.Unlock()

	ss.lastRequestTime = time.Now()
	ss.lockCount++

	if _, have := ss.deviceOwner[device]; have {
		panic("device shouldn't be owned when Load() is called")
	}
	ss.deviceOwner[device] = ordinal
	return nil
}
func (ss *dummyStatefulServer) Unload(device uint64, ordinal uint32) {
	ss.mutex.Lock()
	defer ss.mutex.Unlock()

	ss.lastRequestTime = time.Now()
	ss.unlockCount++

	if owner, have := ss.deviceOwner[device]; !have || owner != ordinal {
		panic("device should be owned when Unload() is called")
	}
	delete(ss.deviceOwner, device)
}
func (ss *dummyStatefulServer) Test(ctx context.Context, r *test.TestRequest, ordinal uint32) (*empty.Empty, error) {
	ss.mutex.Lock()
	defer ss.mutex.Unlock()

	ss.lastRequestTime = time.Now()
	ss.requestCount++
	if owner, have := ss.deviceOwner[r.Device]; !have || owner != ordinal {
		panic("device should be owned when Test() is called")
	}
	if ss.fail {
		return &empty.Empty{}, errors.New("test error")
	}
	return &empty.Empty{}, nil
}
