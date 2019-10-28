// Code generated by protoc-gen-go. DO NOT EDIT.
// source: peer/peer.proto

package peer

import (
	context "context"
	fmt "fmt"
	proto "github.com/golang/protobuf/proto"
	empty "github.com/golang/protobuf/ptypes/empty"
	grpc "google.golang.org/grpc"
	codes "google.golang.org/grpc/codes"
	status "google.golang.org/grpc/status"
	math "math"
)

// Reference imports to suppress errors if they are not otherwise used.
var _ = proto.Marshal
var _ = fmt.Errorf
var _ = math.Inf

// This is a compile-time assertion to ensure that this generated file
// is compatible with the proto package it is being compiled against.
// A compilation error at this line likely means your copy of the
// proto package needs to be updated.
const _ = proto.ProtoPackageIsVersion3 // please upgrade the proto package

type HelloRequest struct {
	Ordinal              uint32   `protobuf:"varint,1,opt,name=ordinal,proto3" json:"ordinal,omitempty"`
	XXX_NoUnkeyedLiteral struct{} `json:"-"`
	XXX_unrecognized     []byte   `json:"-"`
	XXX_sizecache        int32    `json:"-"`
}

func (m *HelloRequest) Reset()         { *m = HelloRequest{} }
func (m *HelloRequest) String() string { return proto.CompactTextString(m) }
func (*HelloRequest) ProtoMessage()    {}
func (*HelloRequest) Descriptor() ([]byte, []int) {
	return fileDescriptor_c302117fbb08ad42, []int{0}
}

func (m *HelloRequest) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_HelloRequest.Unmarshal(m, b)
}
func (m *HelloRequest) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_HelloRequest.Marshal(b, m, deterministic)
}
func (m *HelloRequest) XXX_Merge(src proto.Message) {
	xxx_messageInfo_HelloRequest.Merge(m, src)
}
func (m *HelloRequest) XXX_Size() int {
	return xxx_messageInfo_HelloRequest.Size(m)
}
func (m *HelloRequest) XXX_DiscardUnknown() {
	xxx_messageInfo_HelloRequest.DiscardUnknown(m)
}

var xxx_messageInfo_HelloRequest proto.InternalMessageInfo

func (m *HelloRequest) GetOrdinal() uint32 {
	if m != nil {
		return m.Ordinal
	}
	return 0
}

type NextDeviceRequest struct {
	Ordinal              uint32   `protobuf:"varint,1,opt,name=ordinal,proto3" json:"ordinal,omitempty"`
	Readiness            []byte   `protobuf:"bytes,2,opt,name=readiness,proto3" json:"readiness,omitempty"`
	ReadyForEqual        bool     `protobuf:"varint,3,opt,name=ready_for_equal,json=readyForEqual,proto3" json:"ready_for_equal,omitempty"`
	ReadinessMax         bool     `protobuf:"varint,4,opt,name=readiness_max,json=readinessMax,proto3" json:"readiness_max,omitempty"`
	XXX_NoUnkeyedLiteral struct{} `json:"-"`
	XXX_unrecognized     []byte   `json:"-"`
	XXX_sizecache        int32    `json:"-"`
}

func (m *NextDeviceRequest) Reset()         { *m = NextDeviceRequest{} }
func (m *NextDeviceRequest) String() string { return proto.CompactTextString(m) }
func (*NextDeviceRequest) ProtoMessage()    {}
func (*NextDeviceRequest) Descriptor() ([]byte, []int) {
	return fileDescriptor_c302117fbb08ad42, []int{1}
}

func (m *NextDeviceRequest) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_NextDeviceRequest.Unmarshal(m, b)
}
func (m *NextDeviceRequest) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_NextDeviceRequest.Marshal(b, m, deterministic)
}
func (m *NextDeviceRequest) XXX_Merge(src proto.Message) {
	xxx_messageInfo_NextDeviceRequest.Merge(m, src)
}
func (m *NextDeviceRequest) XXX_Size() int {
	return xxx_messageInfo_NextDeviceRequest.Size(m)
}
func (m *NextDeviceRequest) XXX_DiscardUnknown() {
	xxx_messageInfo_NextDeviceRequest.DiscardUnknown(m)
}

var xxx_messageInfo_NextDeviceRequest proto.InternalMessageInfo

func (m *NextDeviceRequest) GetOrdinal() uint32 {
	if m != nil {
		return m.Ordinal
	}
	return 0
}

func (m *NextDeviceRequest) GetReadiness() []byte {
	if m != nil {
		return m.Readiness
	}
	return nil
}

func (m *NextDeviceRequest) GetReadyForEqual() bool {
	if m != nil {
		return m.ReadyForEqual
	}
	return false
}

func (m *NextDeviceRequest) GetReadinessMax() bool {
	if m != nil {
		return m.ReadinessMax
	}
	return false
}

type NextDeviceResponse struct {
	Has                  bool     `protobuf:"varint,1,opt,name=has,proto3" json:"has,omitempty"`
	Last                 bool     `protobuf:"varint,2,opt,name=last,proto3" json:"last,omitempty"`
	Device               []byte   `protobuf:"bytes,3,opt,name=device,proto3" json:"device,omitempty"`
	XXX_NoUnkeyedLiteral struct{} `json:"-"`
	XXX_unrecognized     []byte   `json:"-"`
	XXX_sizecache        int32    `json:"-"`
}

func (m *NextDeviceResponse) Reset()         { *m = NextDeviceResponse{} }
func (m *NextDeviceResponse) String() string { return proto.CompactTextString(m) }
func (*NextDeviceResponse) ProtoMessage()    {}
func (*NextDeviceResponse) Descriptor() ([]byte, []int) {
	return fileDescriptor_c302117fbb08ad42, []int{2}
}

func (m *NextDeviceResponse) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_NextDeviceResponse.Unmarshal(m, b)
}
func (m *NextDeviceResponse) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_NextDeviceResponse.Marshal(b, m, deterministic)
}
func (m *NextDeviceResponse) XXX_Merge(src proto.Message) {
	xxx_messageInfo_NextDeviceResponse.Merge(m, src)
}
func (m *NextDeviceResponse) XXX_Size() int {
	return xxx_messageInfo_NextDeviceResponse.Size(m)
}
func (m *NextDeviceResponse) XXX_DiscardUnknown() {
	xxx_messageInfo_NextDeviceResponse.DiscardUnknown(m)
}

var xxx_messageInfo_NextDeviceResponse proto.InternalMessageInfo

func (m *NextDeviceResponse) GetHas() bool {
	if m != nil {
		return m.Has
	}
	return false
}

func (m *NextDeviceResponse) GetLast() bool {
	if m != nil {
		return m.Last
	}
	return false
}

func (m *NextDeviceResponse) GetDevice() []byte {
	if m != nil {
		return m.Device
	}
	return nil
}

type ReadinessRequest struct {
	Ordinal              uint32   `protobuf:"varint,1,opt,name=ordinal,proto3" json:"ordinal,omitempty"`
	Readiness            []byte   `protobuf:"bytes,2,opt,name=readiness,proto3" json:"readiness,omitempty"`
	ReadyForEqual        bool     `protobuf:"varint,3,opt,name=ready_for_equal,json=readyForEqual,proto3" json:"ready_for_equal,omitempty"`
	ReadinessMax         bool     `protobuf:"varint,4,opt,name=readiness_max,json=readinessMax,proto3" json:"readiness_max,omitempty"`
	ShuttingDown         bool     `protobuf:"varint,5,opt,name=shutting_down,json=shuttingDown,proto3" json:"shutting_down,omitempty"`
	XXX_NoUnkeyedLiteral struct{} `json:"-"`
	XXX_unrecognized     []byte   `json:"-"`
	XXX_sizecache        int32    `json:"-"`
}

func (m *ReadinessRequest) Reset()         { *m = ReadinessRequest{} }
func (m *ReadinessRequest) String() string { return proto.CompactTextString(m) }
func (*ReadinessRequest) ProtoMessage()    {}
func (*ReadinessRequest) Descriptor() ([]byte, []int) {
	return fileDescriptor_c302117fbb08ad42, []int{3}
}

func (m *ReadinessRequest) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_ReadinessRequest.Unmarshal(m, b)
}
func (m *ReadinessRequest) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_ReadinessRequest.Marshal(b, m, deterministic)
}
func (m *ReadinessRequest) XXX_Merge(src proto.Message) {
	xxx_messageInfo_ReadinessRequest.Merge(m, src)
}
func (m *ReadinessRequest) XXX_Size() int {
	return xxx_messageInfo_ReadinessRequest.Size(m)
}
func (m *ReadinessRequest) XXX_DiscardUnknown() {
	xxx_messageInfo_ReadinessRequest.DiscardUnknown(m)
}

var xxx_messageInfo_ReadinessRequest proto.InternalMessageInfo

func (m *ReadinessRequest) GetOrdinal() uint32 {
	if m != nil {
		return m.Ordinal
	}
	return 0
}

func (m *ReadinessRequest) GetReadiness() []byte {
	if m != nil {
		return m.Readiness
	}
	return nil
}

func (m *ReadinessRequest) GetReadyForEqual() bool {
	if m != nil {
		return m.ReadyForEqual
	}
	return false
}

func (m *ReadinessRequest) GetReadinessMax() bool {
	if m != nil {
		return m.ReadinessMax
	}
	return false
}

func (m *ReadinessRequest) GetShuttingDown() bool {
	if m != nil {
		return m.ShuttingDown
	}
	return false
}

type StatsRequest struct {
	DeviceCounts         map[uint32]uint32 `protobuf:"bytes,1,rep,name=device_counts,json=deviceCounts,proto3" json:"device_counts,omitempty" protobuf_key:"varint,1,opt,name=key,proto3" protobuf_val:"varint,2,opt,name=value,proto3"`
	XXX_NoUnkeyedLiteral struct{}          `json:"-"`
	XXX_unrecognized     []byte            `json:"-"`
	XXX_sizecache        int32             `json:"-"`
}

func (m *StatsRequest) Reset()         { *m = StatsRequest{} }
func (m *StatsRequest) String() string { return proto.CompactTextString(m) }
func (*StatsRequest) ProtoMessage()    {}
func (*StatsRequest) Descriptor() ([]byte, []int) {
	return fileDescriptor_c302117fbb08ad42, []int{4}
}

func (m *StatsRequest) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_StatsRequest.Unmarshal(m, b)
}
func (m *StatsRequest) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_StatsRequest.Marshal(b, m, deterministic)
}
func (m *StatsRequest) XXX_Merge(src proto.Message) {
	xxx_messageInfo_StatsRequest.Merge(m, src)
}
func (m *StatsRequest) XXX_Size() int {
	return xxx_messageInfo_StatsRequest.Size(m)
}
func (m *StatsRequest) XXX_DiscardUnknown() {
	xxx_messageInfo_StatsRequest.DiscardUnknown(m)
}

var xxx_messageInfo_StatsRequest proto.InternalMessageInfo

func (m *StatsRequest) GetDeviceCounts() map[uint32]uint32 {
	if m != nil {
		return m.DeviceCounts
	}
	return nil
}

type HandoffRequest struct {
	Device               []byte   `protobuf:"bytes,1,opt,name=device,proto3" json:"device,omitempty"`
	Ordinal              uint32   `protobuf:"varint,2,opt,name=ordinal,proto3" json:"ordinal,omitempty"`
	Devices              uint32   `protobuf:"varint,3,opt,name=devices,proto3" json:"devices,omitempty"`
	XXX_NoUnkeyedLiteral struct{} `json:"-"`
	XXX_unrecognized     []byte   `json:"-"`
	XXX_sizecache        int32    `json:"-"`
}

func (m *HandoffRequest) Reset()         { *m = HandoffRequest{} }
func (m *HandoffRequest) String() string { return proto.CompactTextString(m) }
func (*HandoffRequest) ProtoMessage()    {}
func (*HandoffRequest) Descriptor() ([]byte, []int) {
	return fileDescriptor_c302117fbb08ad42, []int{5}
}

func (m *HandoffRequest) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_HandoffRequest.Unmarshal(m, b)
}
func (m *HandoffRequest) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_HandoffRequest.Marshal(b, m, deterministic)
}
func (m *HandoffRequest) XXX_Merge(src proto.Message) {
	xxx_messageInfo_HandoffRequest.Merge(m, src)
}
func (m *HandoffRequest) XXX_Size() int {
	return xxx_messageInfo_HandoffRequest.Size(m)
}
func (m *HandoffRequest) XXX_DiscardUnknown() {
	xxx_messageInfo_HandoffRequest.DiscardUnknown(m)
}

var xxx_messageInfo_HandoffRequest proto.InternalMessageInfo

func (m *HandoffRequest) GetDevice() []byte {
	if m != nil {
		return m.Device
	}
	return nil
}

func (m *HandoffRequest) GetOrdinal() uint32 {
	if m != nil {
		return m.Ordinal
	}
	return 0
}

func (m *HandoffRequest) GetDevices() uint32 {
	if m != nil {
		return m.Devices
	}
	return 0
}

func init() {
	proto.RegisterType((*HelloRequest)(nil), "peer.HelloRequest")
	proto.RegisterType((*NextDeviceRequest)(nil), "peer.NextDeviceRequest")
	proto.RegisterType((*NextDeviceResponse)(nil), "peer.NextDeviceResponse")
	proto.RegisterType((*ReadinessRequest)(nil), "peer.ReadinessRequest")
	proto.RegisterType((*StatsRequest)(nil), "peer.StatsRequest")
	proto.RegisterMapType((map[uint32]uint32)(nil), "peer.StatsRequest.DeviceCountsEntry")
	proto.RegisterType((*HandoffRequest)(nil), "peer.HandoffRequest")
}

func init() { proto.RegisterFile("peer/peer.proto", fileDescriptor_c302117fbb08ad42) }

var fileDescriptor_c302117fbb08ad42 = []byte{
	// 486 bytes of a gzipped FileDescriptorProto
	0x1f, 0x8b, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, 0xff, 0xcc, 0x53, 0x4d, 0x6f, 0xd3, 0x40,
	0x10, 0xad, 0xf3, 0xd1, 0x84, 0xa9, 0x4d, 0xda, 0x51, 0x15, 0xac, 0xc0, 0x21, 0x32, 0x08, 0xe5,
	0xe4, 0x48, 0x45, 0x48, 0x7c, 0x1c, 0x10, 0x6a, 0x83, 0xca, 0x01, 0x84, 0x16, 0x71, 0x43, 0xb2,
	0xb6, 0xf5, 0x24, 0x8d, 0x70, 0x77, 0x5d, 0xef, 0xba, 0x4d, 0xfe, 0x09, 0xe2, 0xb7, 0xc0, 0x7f,
	0x43, 0xbb, 0x1b, 0x27, 0x2e, 0x11, 0xe5, 0xca, 0x25, 0x9a, 0x79, 0x79, 0xe3, 0x9d, 0x79, 0xf3,
	0x06, 0x7a, 0x39, 0x51, 0x31, 0x36, 0x3f, 0x71, 0x5e, 0x48, 0x2d, 0xb1, 0x65, 0xe2, 0xc1, 0xc3,
	0x99, 0x94, 0xb3, 0x8c, 0xc6, 0x16, 0x3b, 0x2b, 0xa7, 0x63, 0xba, 0xcc, 0xf5, 0xd2, 0x51, 0xa2,
	0x11, 0xf8, 0xa7, 0x94, 0x65, 0x92, 0xd1, 0x55, 0x49, 0x4a, 0x63, 0x08, 0x1d, 0x59, 0xa4, 0x73,
	0xc1, 0xb3, 0xd0, 0x1b, 0x7a, 0xa3, 0x80, 0x55, 0x69, 0xf4, 0xdd, 0x83, 0x83, 0x8f, 0xb4, 0xd0,
	0x27, 0x74, 0x3d, 0x3f, 0xa7, 0x7f, 0xf2, 0xf1, 0x11, 0xdc, 0x2b, 0x88, 0xa7, 0x73, 0x41, 0x4a,
	0x85, 0x8d, 0xa1, 0x37, 0xf2, 0xd9, 0x06, 0xc0, 0xa7, 0xd0, 0x33, 0xc9, 0x32, 0x99, 0xca, 0x22,
	0xa1, 0xab, 0x92, 0x67, 0x61, 0x73, 0xe8, 0x8d, 0xba, 0x2c, 0xb0, 0xf0, 0x3b, 0x59, 0x4c, 0x0c,
	0x88, 0x8f, 0x21, 0x58, 0x17, 0x25, 0x97, 0x7c, 0x11, 0xb6, 0x2c, 0xcb, 0x5f, 0x83, 0x1f, 0xf8,
	0x22, 0x62, 0x80, 0xf5, 0xce, 0x54, 0x2e, 0x85, 0x22, 0xdc, 0x87, 0xe6, 0x05, 0x57, 0xb6, 0xad,
	0x2e, 0x33, 0x21, 0x22, 0xb4, 0x32, 0xae, 0xb4, 0xed, 0xa6, 0xcb, 0x6c, 0x8c, 0x7d, 0xd8, 0x4d,
	0x6d, 0x9d, 0x7d, 0xdf, 0x67, 0xab, 0x2c, 0xfa, 0xe9, 0xc1, 0x3e, 0xab, 0x1e, 0xf9, 0x9f, 0xa6,
	0x35, 0x24, 0x75, 0x51, 0x6a, 0x3d, 0x17, 0xb3, 0x24, 0x95, 0x37, 0x22, 0x6c, 0x3b, 0x52, 0x05,
	0x9e, 0xc8, 0x1b, 0x11, 0xfd, 0xf0, 0xc0, 0xff, 0xac, 0xb9, 0x5e, 0xb7, 0xfe, 0x1e, 0x02, 0x37,
	0x59, 0x72, 0x2e, 0x4b, 0xa1, 0x8d, 0x2e, 0xcd, 0xd1, 0xde, 0xd1, 0x93, 0xd8, 0xfa, 0xa5, 0x4e,
	0x8d, 0x9d, 0x8e, 0xc7, 0x96, 0x36, 0x11, 0xba, 0x58, 0x32, 0x3f, 0xad, 0x41, 0x83, 0x37, 0x70,
	0xb0, 0x45, 0x31, 0x6a, 0x7f, 0xa3, 0xe5, 0x4a, 0x16, 0x13, 0xe2, 0x21, 0xb4, 0xaf, 0x79, 0x56,
	0x92, 0x95, 0x23, 0x60, 0x2e, 0x79, 0xd5, 0x78, 0xe1, 0x45, 0x5f, 0xe1, 0xfe, 0x29, 0x17, 0xa9,
	0x9c, 0x4e, 0xab, 0xee, 0x36, 0x5b, 0xf0, 0xea, 0x5b, 0xa8, 0x0b, 0xde, 0xb8, 0x2d, 0x78, 0x08,
	0x1d, 0xc7, 0x51, 0x56, 0xca, 0x80, 0x55, 0xe9, 0xd1, 0xaf, 0x06, 0xb4, 0x3e, 0x11, 0x15, 0xf8,
	0x1c, 0xda, 0xd6, 0xdb, 0x88, 0x6e, 0xc8, 0xba, 0xd1, 0x07, 0xfd, 0xd8, 0x9d, 0x45, 0x5c, 0x9d,
	0x45, 0x3c, 0x31, 0x67, 0x11, 0xed, 0xe0, 0x5b, 0x80, 0x8d, 0x9b, 0xf0, 0x81, 0xab, 0xdd, 0x72,
	0xfe, 0x20, 0xdc, 0xfe, 0xc3, 0x19, 0x2f, 0xda, 0xc1, 0x63, 0xe8, 0x7d, 0xc9, 0x53, 0xae, 0x69,
	0xed, 0x20, 0xec, 0x3b, 0xfa, 0x9f, 0x96, 0xba, 0xa3, 0x8f, 0xd7, 0xb0, 0xe7, 0x3e, 0x62, 0x97,
	0x53, 0x0d, 0x51, 0xdf, 0xd4, 0x1d, 0xc5, 0x2f, 0xa1, 0xb3, 0x92, 0x18, 0x0f, 0x57, 0xd3, 0xdf,
	0x52, 0xfc, 0xef, 0xa5, 0x67, 0xbb, 0x16, 0x79, 0xf6, 0x3b, 0x00, 0x00, 0xff, 0xff, 0xd4, 0x7d,
	0x00, 0xc4, 0x4f, 0x04, 0x00, 0x00,
}

// Reference imports to suppress errors if they are not otherwise used.
var _ context.Context
var _ grpc.ClientConn

// This is a compile-time assertion to ensure that this generated file
// is compatible with the grpc package it is being compiled against.
const _ = grpc.SupportPackageIsVersion4

// PeerClient is the client API for Peer service.
//
// For semantics around ctx use and closing/ending streaming RPCs, please refer to https://godoc.org/google.golang.org/grpc#ClientConn.NewStream.
type PeerClient interface {
	Hello(ctx context.Context, in *HelloRequest, opts ...grpc.CallOption) (*empty.Empty, error)
	NextDevice(ctx context.Context, in *NextDeviceRequest, opts ...grpc.CallOption) (*NextDeviceResponse, error)
	UpdateReadiness(ctx context.Context, in *ReadinessRequest, opts ...grpc.CallOption) (*empty.Empty, error)
	UpdateStats(ctx context.Context, in *StatsRequest, opts ...grpc.CallOption) (*empty.Empty, error)
	Handoff(ctx context.Context, in *HandoffRequest, opts ...grpc.CallOption) (*empty.Empty, error)
}

type peerClient struct {
	cc *grpc.ClientConn
}

func NewPeerClient(cc *grpc.ClientConn) PeerClient {
	return &peerClient{cc}
}

func (c *peerClient) Hello(ctx context.Context, in *HelloRequest, opts ...grpc.CallOption) (*empty.Empty, error) {
	out := new(empty.Empty)
	err := c.cc.Invoke(ctx, "/peer.Peer/Hello", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *peerClient) NextDevice(ctx context.Context, in *NextDeviceRequest, opts ...grpc.CallOption) (*NextDeviceResponse, error) {
	out := new(NextDeviceResponse)
	err := c.cc.Invoke(ctx, "/peer.Peer/NextDevice", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *peerClient) UpdateReadiness(ctx context.Context, in *ReadinessRequest, opts ...grpc.CallOption) (*empty.Empty, error) {
	out := new(empty.Empty)
	err := c.cc.Invoke(ctx, "/peer.Peer/UpdateReadiness", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *peerClient) UpdateStats(ctx context.Context, in *StatsRequest, opts ...grpc.CallOption) (*empty.Empty, error) {
	out := new(empty.Empty)
	err := c.cc.Invoke(ctx, "/peer.Peer/UpdateStats", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *peerClient) Handoff(ctx context.Context, in *HandoffRequest, opts ...grpc.CallOption) (*empty.Empty, error) {
	out := new(empty.Empty)
	err := c.cc.Invoke(ctx, "/peer.Peer/Handoff", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

// PeerServer is the server API for Peer service.
type PeerServer interface {
	Hello(context.Context, *HelloRequest) (*empty.Empty, error)
	NextDevice(context.Context, *NextDeviceRequest) (*NextDeviceResponse, error)
	UpdateReadiness(context.Context, *ReadinessRequest) (*empty.Empty, error)
	UpdateStats(context.Context, *StatsRequest) (*empty.Empty, error)
	Handoff(context.Context, *HandoffRequest) (*empty.Empty, error)
}

// UnimplementedPeerServer can be embedded to have forward compatible implementations.
type UnimplementedPeerServer struct {
}

func (*UnimplementedPeerServer) Hello(ctx context.Context, req *HelloRequest) (*empty.Empty, error) {
	return nil, status.Errorf(codes.Unimplemented, "method Hello not implemented")
}
func (*UnimplementedPeerServer) NextDevice(ctx context.Context, req *NextDeviceRequest) (*NextDeviceResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method NextDevice not implemented")
}
func (*UnimplementedPeerServer) UpdateReadiness(ctx context.Context, req *ReadinessRequest) (*empty.Empty, error) {
	return nil, status.Errorf(codes.Unimplemented, "method UpdateReadiness not implemented")
}
func (*UnimplementedPeerServer) UpdateStats(ctx context.Context, req *StatsRequest) (*empty.Empty, error) {
	return nil, status.Errorf(codes.Unimplemented, "method UpdateStats not implemented")
}
func (*UnimplementedPeerServer) Handoff(ctx context.Context, req *HandoffRequest) (*empty.Empty, error) {
	return nil, status.Errorf(codes.Unimplemented, "method Handoff not implemented")
}

func RegisterPeerServer(s *grpc.Server, srv PeerServer) {
	s.RegisterService(&_Peer_serviceDesc, srv)
}

func _Peer_Hello_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(HelloRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(PeerServer).Hello(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/peer.Peer/Hello",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(PeerServer).Hello(ctx, req.(*HelloRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _Peer_NextDevice_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(NextDeviceRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(PeerServer).NextDevice(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/peer.Peer/NextDevice",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(PeerServer).NextDevice(ctx, req.(*NextDeviceRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _Peer_UpdateReadiness_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(ReadinessRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(PeerServer).UpdateReadiness(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/peer.Peer/UpdateReadiness",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(PeerServer).UpdateReadiness(ctx, req.(*ReadinessRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _Peer_UpdateStats_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(StatsRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(PeerServer).UpdateStats(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/peer.Peer/UpdateStats",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(PeerServer).UpdateStats(ctx, req.(*StatsRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _Peer_Handoff_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(HandoffRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(PeerServer).Handoff(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/peer.Peer/Handoff",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(PeerServer).Handoff(ctx, req.(*HandoffRequest))
	}
	return interceptor(ctx, in, info, handler)
}

var _Peer_serviceDesc = grpc.ServiceDesc{
	ServiceName: "peer.Peer",
	HandlerType: (*PeerServer)(nil),
	Methods: []grpc.MethodDesc{
		{
			MethodName: "Hello",
			Handler:    _Peer_Hello_Handler,
		},
		{
			MethodName: "NextDevice",
			Handler:    _Peer_NextDevice_Handler,
		},
		{
			MethodName: "UpdateReadiness",
			Handler:    _Peer_UpdateReadiness_Handler,
		},
		{
			MethodName: "UpdateStats",
			Handler:    _Peer_UpdateStats_Handler,
		},
		{
			MethodName: "Handoff",
			Handler:    _Peer_Handoff_Handler,
		},
	},
	Streams:  []grpc.StreamDesc{},
	Metadata: "peer/peer.proto",
}
