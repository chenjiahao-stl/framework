// Code generated by protoc-gen-go. DO NOT EDIT.
// versions:
// 	protoc-gen-go v1.34.2
// 	protoc        v5.26.0--rc3
// source: common.proto

package conf

import (
	protoreflect "google.golang.org/protobuf/reflect/protoreflect"
	protoimpl "google.golang.org/protobuf/runtime/protoimpl"
	durationpb "google.golang.org/protobuf/types/known/durationpb"
	reflect "reflect"
	sync "sync"
)

const (
	// Verify that this generated code is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(20 - protoimpl.MinVersion)
	// Verify that runtime/protoimpl is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(protoimpl.MaxVersion - 20)
)

type Bootstrap struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	ConfigCenter *ConfigCenter `protobuf:"bytes,1,opt,name=config_center,json=configCenter,proto3" json:"config_center,omitempty"`
	Registry     *Registry     `protobuf:"bytes,2,opt,name=registry,proto3" json:"registry,omitempty"`
	Data         *Data         `protobuf:"bytes,3,opt,name=data,proto3" json:"data,omitempty"`
}

func (x *Bootstrap) Reset() {
	*x = Bootstrap{}
	if protoimpl.UnsafeEnabled {
		mi := &file_common_proto_msgTypes[0]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *Bootstrap) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*Bootstrap) ProtoMessage() {}

func (x *Bootstrap) ProtoReflect() protoreflect.Message {
	mi := &file_common_proto_msgTypes[0]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use Bootstrap.ProtoReflect.Descriptor instead.
func (*Bootstrap) Descriptor() ([]byte, []int) {
	return file_common_proto_rawDescGZIP(), []int{0}
}

func (x *Bootstrap) GetConfigCenter() *ConfigCenter {
	if x != nil {
		return x.ConfigCenter
	}
	return nil
}

func (x *Bootstrap) GetRegistry() *Registry {
	if x != nil {
		return x.Registry
	}
	return nil
}

func (x *Bootstrap) GetData() *Data {
	if x != nil {
		return x.Data
	}
	return nil
}

// 配置中心
type ConfigCenter struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Nacos *ConfigCenter_Nacos `protobuf:"bytes,1,opt,name=nacos,proto3" json:"nacos,omitempty"`
}

func (x *ConfigCenter) Reset() {
	*x = ConfigCenter{}
	if protoimpl.UnsafeEnabled {
		mi := &file_common_proto_msgTypes[1]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *ConfigCenter) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*ConfigCenter) ProtoMessage() {}

func (x *ConfigCenter) ProtoReflect() protoreflect.Message {
	mi := &file_common_proto_msgTypes[1]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use ConfigCenter.ProtoReflect.Descriptor instead.
func (*ConfigCenter) Descriptor() ([]byte, []int) {
	return file_common_proto_rawDescGZIP(), []int{1}
}

func (x *ConfigCenter) GetNacos() *ConfigCenter_Nacos {
	if x != nil {
		return x.Nacos
	}
	return nil
}

// 注册中心
type Registry struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Namespace string         `protobuf:"bytes,1,opt,name=namespace,proto3" json:"namespace,omitempty"`
	Etcd      *Registry_Etcd `protobuf:"bytes,2,opt,name=etcd,proto3" json:"etcd,omitempty"`
}

func (x *Registry) Reset() {
	*x = Registry{}
	if protoimpl.UnsafeEnabled {
		mi := &file_common_proto_msgTypes[2]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *Registry) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*Registry) ProtoMessage() {}

func (x *Registry) ProtoReflect() protoreflect.Message {
	mi := &file_common_proto_msgTypes[2]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use Registry.ProtoReflect.Descriptor instead.
func (*Registry) Descriptor() ([]byte, []int) {
	return file_common_proto_rawDescGZIP(), []int{2}
}

func (x *Registry) GetNamespace() string {
	if x != nil {
		return x.Namespace
	}
	return ""
}

func (x *Registry) GetEtcd() *Registry_Etcd {
	if x != nil {
		return x.Etcd
	}
	return nil
}

type Data struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Redis  *Data_Redis  `protobuf:"bytes,1,opt,name=redis,proto3" json:"redis,omitempty"`
	Etcd   *Data_Etcd   `protobuf:"bytes,2,opt,name=etcd,proto3" json:"etcd,omitempty"`
	Kafka  *Data_Kafka  `protobuf:"bytes,3,opt,name=kafka,proto3" json:"kafka,omitempty"`
	Tracer *Data_Tracer `protobuf:"bytes,4,opt,name=tracer,proto3" json:"tracer,omitempty"`
}

func (x *Data) Reset() {
	*x = Data{}
	if protoimpl.UnsafeEnabled {
		mi := &file_common_proto_msgTypes[3]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *Data) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*Data) ProtoMessage() {}

func (x *Data) ProtoReflect() protoreflect.Message {
	mi := &file_common_proto_msgTypes[3]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use Data.ProtoReflect.Descriptor instead.
func (*Data) Descriptor() ([]byte, []int) {
	return file_common_proto_rawDescGZIP(), []int{3}
}

func (x *Data) GetRedis() *Data_Redis {
	if x != nil {
		return x.Redis
	}
	return nil
}

func (x *Data) GetEtcd() *Data_Etcd {
	if x != nil {
		return x.Etcd
	}
	return nil
}

func (x *Data) GetKafka() *Data_Kafka {
	if x != nil {
		return x.Kafka
	}
	return nil
}

func (x *Data) GetTracer() *Data_Tracer {
	if x != nil {
		return x.Tracer
	}
	return nil
}

type ConfigCenter_Nacos struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Host        string `protobuf:"bytes,1,opt,name=host,proto3" json:"host,omitempty"`
	Port        uint64 `protobuf:"varint,2,opt,name=port,proto3" json:"port,omitempty"`
	Username    string `protobuf:"bytes,3,opt,name=username,proto3" json:"username,omitempty"`
	Password    string `protobuf:"bytes,4,opt,name=password,proto3" json:"password,omitempty"`
	NamespaceId string `protobuf:"bytes,5,opt,name=namespace_id,json=namespaceId,proto3" json:"namespace_id,omitempty"`
	Group       string `protobuf:"bytes,6,opt,name=group,proto3" json:"group,omitempty"`
}

func (x *ConfigCenter_Nacos) Reset() {
	*x = ConfigCenter_Nacos{}
	if protoimpl.UnsafeEnabled {
		mi := &file_common_proto_msgTypes[4]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *ConfigCenter_Nacos) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*ConfigCenter_Nacos) ProtoMessage() {}

func (x *ConfigCenter_Nacos) ProtoReflect() protoreflect.Message {
	mi := &file_common_proto_msgTypes[4]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use ConfigCenter_Nacos.ProtoReflect.Descriptor instead.
func (*ConfigCenter_Nacos) Descriptor() ([]byte, []int) {
	return file_common_proto_rawDescGZIP(), []int{1, 0}
}

func (x *ConfigCenter_Nacos) GetHost() string {
	if x != nil {
		return x.Host
	}
	return ""
}

func (x *ConfigCenter_Nacos) GetPort() uint64 {
	if x != nil {
		return x.Port
	}
	return 0
}

func (x *ConfigCenter_Nacos) GetUsername() string {
	if x != nil {
		return x.Username
	}
	return ""
}

func (x *ConfigCenter_Nacos) GetPassword() string {
	if x != nil {
		return x.Password
	}
	return ""
}

func (x *ConfigCenter_Nacos) GetNamespaceId() string {
	if x != nil {
		return x.NamespaceId
	}
	return ""
}

func (x *ConfigCenter_Nacos) GetGroup() string {
	if x != nil {
		return x.Group
	}
	return ""
}

type Registry_Etcd struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	MaxRetry int32 `protobuf:"varint,1,opt,name=max_retry,json=maxRetry,proto3" json:"max_retry,omitempty"`
}

func (x *Registry_Etcd) Reset() {
	*x = Registry_Etcd{}
	if protoimpl.UnsafeEnabled {
		mi := &file_common_proto_msgTypes[5]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *Registry_Etcd) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*Registry_Etcd) ProtoMessage() {}

func (x *Registry_Etcd) ProtoReflect() protoreflect.Message {
	mi := &file_common_proto_msgTypes[5]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use Registry_Etcd.ProtoReflect.Descriptor instead.
func (*Registry_Etcd) Descriptor() ([]byte, []int) {
	return file_common_proto_rawDescGZIP(), []int{2, 0}
}

func (x *Registry_Etcd) GetMaxRetry() int32 {
	if x != nil {
		return x.MaxRetry
	}
	return 0
}

type Data_Redis struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Primary          string               `protobuf:"bytes,1,opt,name=primary,proto3" json:"primary,omitempty"`
	Host             string               `protobuf:"bytes,2,opt,name=host,proto3" json:"host,omitempty"`
	Port             uint64               `protobuf:"varint,3,opt,name=port,proto3" json:"port,omitempty"`
	Password         string               `protobuf:"bytes,4,opt,name=password,proto3" json:"password,omitempty"`
	Db               uint64               `protobuf:"varint,5,opt,name=db,proto3" json:"db,omitempty"`
	Username         string               `protobuf:"bytes,6,opt,name=username,proto3" json:"username,omitempty"`
	SentinelUsername string               `protobuf:"bytes,7,opt,name=sentinel_username,json=sentinelUsername,proto3" json:"sentinel_username,omitempty"`
	SentinelPassword string               `protobuf:"bytes,8,opt,name=sentinel_password,json=sentinelPassword,proto3" json:"sentinel_password,omitempty"`
	PoolSize         uint64               `protobuf:"varint,9,opt,name=pool_size,json=poolSize,proto3" json:"pool_size,omitempty"`
	MinIdleConns     uint64               `protobuf:"varint,10,opt,name=min_idle_conns,json=minIdleConns,proto3" json:"min_idle_conns,omitempty"`
	ReadTimeout      *durationpb.Duration `protobuf:"bytes,11,opt,name=read_timeout,json=readTimeout,proto3" json:"read_timeout,omitempty"`
	WriteTimeout     *durationpb.Duration `protobuf:"bytes,12,opt,name=write_timeout,json=writeTimeout,proto3" json:"write_timeout,omitempty"`
}

func (x *Data_Redis) Reset() {
	*x = Data_Redis{}
	if protoimpl.UnsafeEnabled {
		mi := &file_common_proto_msgTypes[6]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *Data_Redis) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*Data_Redis) ProtoMessage() {}

func (x *Data_Redis) ProtoReflect() protoreflect.Message {
	mi := &file_common_proto_msgTypes[6]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use Data_Redis.ProtoReflect.Descriptor instead.
func (*Data_Redis) Descriptor() ([]byte, []int) {
	return file_common_proto_rawDescGZIP(), []int{3, 0}
}

func (x *Data_Redis) GetPrimary() string {
	if x != nil {
		return x.Primary
	}
	return ""
}

func (x *Data_Redis) GetHost() string {
	if x != nil {
		return x.Host
	}
	return ""
}

func (x *Data_Redis) GetPort() uint64 {
	if x != nil {
		return x.Port
	}
	return 0
}

func (x *Data_Redis) GetPassword() string {
	if x != nil {
		return x.Password
	}
	return ""
}

func (x *Data_Redis) GetDb() uint64 {
	if x != nil {
		return x.Db
	}
	return 0
}

func (x *Data_Redis) GetUsername() string {
	if x != nil {
		return x.Username
	}
	return ""
}

func (x *Data_Redis) GetSentinelUsername() string {
	if x != nil {
		return x.SentinelUsername
	}
	return ""
}

func (x *Data_Redis) GetSentinelPassword() string {
	if x != nil {
		return x.SentinelPassword
	}
	return ""
}

func (x *Data_Redis) GetPoolSize() uint64 {
	if x != nil {
		return x.PoolSize
	}
	return 0
}

func (x *Data_Redis) GetMinIdleConns() uint64 {
	if x != nil {
		return x.MinIdleConns
	}
	return 0
}

func (x *Data_Redis) GetReadTimeout() *durationpb.Duration {
	if x != nil {
		return x.ReadTimeout
	}
	return nil
}

func (x *Data_Redis) GetWriteTimeout() *durationpb.Duration {
	if x != nil {
		return x.WriteTimeout
	}
	return nil
}

type Data_Etcd struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Host     string `protobuf:"bytes,1,opt,name=host,proto3" json:"host,omitempty"`
	Port     uint64 `protobuf:"varint,2,opt,name=port,proto3" json:"port,omitempty"`
	Username string `protobuf:"bytes,3,opt,name=username,proto3" json:"username,omitempty"`
	Password string `protobuf:"bytes,4,opt,name=password,proto3" json:"password,omitempty"`
}

func (x *Data_Etcd) Reset() {
	*x = Data_Etcd{}
	if protoimpl.UnsafeEnabled {
		mi := &file_common_proto_msgTypes[7]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *Data_Etcd) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*Data_Etcd) ProtoMessage() {}

func (x *Data_Etcd) ProtoReflect() protoreflect.Message {
	mi := &file_common_proto_msgTypes[7]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use Data_Etcd.ProtoReflect.Descriptor instead.
func (*Data_Etcd) Descriptor() ([]byte, []int) {
	return file_common_proto_rawDescGZIP(), []int{3, 1}
}

func (x *Data_Etcd) GetHost() string {
	if x != nil {
		return x.Host
	}
	return ""
}

func (x *Data_Etcd) GetPort() uint64 {
	if x != nil {
		return x.Port
	}
	return 0
}

func (x *Data_Etcd) GetUsername() string {
	if x != nil {
		return x.Username
	}
	return ""
}

func (x *Data_Etcd) GetPassword() string {
	if x != nil {
		return x.Password
	}
	return ""
}

type Data_Kafka struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Host              string `protobuf:"bytes,1,opt,name=host,proto3" json:"host,omitempty"`
	Port              uint64 `protobuf:"varint,2,opt,name=port,proto3" json:"port,omitempty"`
	Username          string `protobuf:"bytes,3,opt,name=username,proto3" json:"username,omitempty"`
	Password          string `protobuf:"bytes,4,opt,name=password,proto3" json:"password,omitempty"`
	ProducerMode      string `protobuf:"bytes,5,opt,name=producer_mode,json=producerMode,proto3" json:"producer_mode,omitempty"`
	Timeout           uint64 `protobuf:"varint,6,opt,name=timeout,proto3" json:"timeout,omitempty"`
	ManualPartitioner string `protobuf:"bytes,7,opt,name=manual_partitioner,json=manualPartitioner,proto3" json:"manual_partitioner,omitempty"`
}

func (x *Data_Kafka) Reset() {
	*x = Data_Kafka{}
	if protoimpl.UnsafeEnabled {
		mi := &file_common_proto_msgTypes[8]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *Data_Kafka) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*Data_Kafka) ProtoMessage() {}

func (x *Data_Kafka) ProtoReflect() protoreflect.Message {
	mi := &file_common_proto_msgTypes[8]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use Data_Kafka.ProtoReflect.Descriptor instead.
func (*Data_Kafka) Descriptor() ([]byte, []int) {
	return file_common_proto_rawDescGZIP(), []int{3, 2}
}

func (x *Data_Kafka) GetHost() string {
	if x != nil {
		return x.Host
	}
	return ""
}

func (x *Data_Kafka) GetPort() uint64 {
	if x != nil {
		return x.Port
	}
	return 0
}

func (x *Data_Kafka) GetUsername() string {
	if x != nil {
		return x.Username
	}
	return ""
}

func (x *Data_Kafka) GetPassword() string {
	if x != nil {
		return x.Password
	}
	return ""
}

func (x *Data_Kafka) GetProducerMode() string {
	if x != nil {
		return x.ProducerMode
	}
	return ""
}

func (x *Data_Kafka) GetTimeout() uint64 {
	if x != nil {
		return x.Timeout
	}
	return 0
}

func (x *Data_Kafka) GetManualPartitioner() string {
	if x != nil {
		return x.ManualPartitioner
	}
	return ""
}

type Data_Tracer struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Url       string `protobuf:"bytes,1,opt,name=url,proto3" json:"url,omitempty"`
	AgentHost string `protobuf:"bytes,2,opt,name=agent_host,json=agentHost,proto3" json:"agent_host,omitempty"`
	AgentPort string `protobuf:"bytes,3,opt,name=agent_port,json=agentPort,proto3" json:"agent_port,omitempty"`
}

func (x *Data_Tracer) Reset() {
	*x = Data_Tracer{}
	if protoimpl.UnsafeEnabled {
		mi := &file_common_proto_msgTypes[9]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *Data_Tracer) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*Data_Tracer) ProtoMessage() {}

func (x *Data_Tracer) ProtoReflect() protoreflect.Message {
	mi := &file_common_proto_msgTypes[9]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use Data_Tracer.ProtoReflect.Descriptor instead.
func (*Data_Tracer) Descriptor() ([]byte, []int) {
	return file_common_proto_rawDescGZIP(), []int{3, 3}
}

func (x *Data_Tracer) GetUrl() string {
	if x != nil {
		return x.Url
	}
	return ""
}

func (x *Data_Tracer) GetAgentHost() string {
	if x != nil {
		return x.AgentHost
	}
	return ""
}

func (x *Data_Tracer) GetAgentPort() string {
	if x != nil {
		return x.AgentPort
	}
	return ""
}

var File_common_proto protoreflect.FileDescriptor

var file_common_proto_rawDesc = []byte{
	0x0a, 0x0c, 0x63, 0x6f, 0x6d, 0x6d, 0x6f, 0x6e, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x12, 0x04,
	0x63, 0x6f, 0x6e, 0x66, 0x1a, 0x1e, 0x67, 0x6f, 0x6f, 0x67, 0x6c, 0x65, 0x2f, 0x70, 0x72, 0x6f,
	0x74, 0x6f, 0x62, 0x75, 0x66, 0x2f, 0x64, 0x75, 0x72, 0x61, 0x74, 0x69, 0x6f, 0x6e, 0x2e, 0x70,
	0x72, 0x6f, 0x74, 0x6f, 0x22, 0x90, 0x01, 0x0a, 0x09, 0x42, 0x6f, 0x6f, 0x74, 0x73, 0x74, 0x72,
	0x61, 0x70, 0x12, 0x37, 0x0a, 0x0d, 0x63, 0x6f, 0x6e, 0x66, 0x69, 0x67, 0x5f, 0x63, 0x65, 0x6e,
	0x74, 0x65, 0x72, 0x18, 0x01, 0x20, 0x01, 0x28, 0x0b, 0x32, 0x12, 0x2e, 0x63, 0x6f, 0x6e, 0x66,
	0x2e, 0x43, 0x6f, 0x6e, 0x66, 0x69, 0x67, 0x43, 0x65, 0x6e, 0x74, 0x65, 0x72, 0x52, 0x0c, 0x63,
	0x6f, 0x6e, 0x66, 0x69, 0x67, 0x43, 0x65, 0x6e, 0x74, 0x65, 0x72, 0x12, 0x2a, 0x0a, 0x08, 0x72,
	0x65, 0x67, 0x69, 0x73, 0x74, 0x72, 0x79, 0x18, 0x02, 0x20, 0x01, 0x28, 0x0b, 0x32, 0x0e, 0x2e,
	0x63, 0x6f, 0x6e, 0x66, 0x2e, 0x52, 0x65, 0x67, 0x69, 0x73, 0x74, 0x72, 0x79, 0x52, 0x08, 0x72,
	0x65, 0x67, 0x69, 0x73, 0x74, 0x72, 0x79, 0x12, 0x1e, 0x0a, 0x04, 0x64, 0x61, 0x74, 0x61, 0x18,
	0x03, 0x20, 0x01, 0x28, 0x0b, 0x32, 0x0a, 0x2e, 0x63, 0x6f, 0x6e, 0x66, 0x2e, 0x44, 0x61, 0x74,
	0x61, 0x52, 0x04, 0x64, 0x61, 0x74, 0x61, 0x22, 0xe1, 0x01, 0x0a, 0x0c, 0x43, 0x6f, 0x6e, 0x66,
	0x69, 0x67, 0x43, 0x65, 0x6e, 0x74, 0x65, 0x72, 0x12, 0x2e, 0x0a, 0x05, 0x6e, 0x61, 0x63, 0x6f,
	0x73, 0x18, 0x01, 0x20, 0x01, 0x28, 0x0b, 0x32, 0x18, 0x2e, 0x63, 0x6f, 0x6e, 0x66, 0x2e, 0x43,
	0x6f, 0x6e, 0x66, 0x69, 0x67, 0x43, 0x65, 0x6e, 0x74, 0x65, 0x72, 0x2e, 0x4e, 0x61, 0x63, 0x6f,
	0x73, 0x52, 0x05, 0x6e, 0x61, 0x63, 0x6f, 0x73, 0x1a, 0xa0, 0x01, 0x0a, 0x05, 0x4e, 0x61, 0x63,
	0x6f, 0x73, 0x12, 0x12, 0x0a, 0x04, 0x68, 0x6f, 0x73, 0x74, 0x18, 0x01, 0x20, 0x01, 0x28, 0x09,
	0x52, 0x04, 0x68, 0x6f, 0x73, 0x74, 0x12, 0x12, 0x0a, 0x04, 0x70, 0x6f, 0x72, 0x74, 0x18, 0x02,
	0x20, 0x01, 0x28, 0x04, 0x52, 0x04, 0x70, 0x6f, 0x72, 0x74, 0x12, 0x1a, 0x0a, 0x08, 0x75, 0x73,
	0x65, 0x72, 0x6e, 0x61, 0x6d, 0x65, 0x18, 0x03, 0x20, 0x01, 0x28, 0x09, 0x52, 0x08, 0x75, 0x73,
	0x65, 0x72, 0x6e, 0x61, 0x6d, 0x65, 0x12, 0x1a, 0x0a, 0x08, 0x70, 0x61, 0x73, 0x73, 0x77, 0x6f,
	0x72, 0x64, 0x18, 0x04, 0x20, 0x01, 0x28, 0x09, 0x52, 0x08, 0x70, 0x61, 0x73, 0x73, 0x77, 0x6f,
	0x72, 0x64, 0x12, 0x21, 0x0a, 0x0c, 0x6e, 0x61, 0x6d, 0x65, 0x73, 0x70, 0x61, 0x63, 0x65, 0x5f,
	0x69, 0x64, 0x18, 0x05, 0x20, 0x01, 0x28, 0x09, 0x52, 0x0b, 0x6e, 0x61, 0x6d, 0x65, 0x73, 0x70,
	0x61, 0x63, 0x65, 0x49, 0x64, 0x12, 0x14, 0x0a, 0x05, 0x67, 0x72, 0x6f, 0x75, 0x70, 0x18, 0x06,
	0x20, 0x01, 0x28, 0x09, 0x52, 0x05, 0x67, 0x72, 0x6f, 0x75, 0x70, 0x22, 0x76, 0x0a, 0x08, 0x52,
	0x65, 0x67, 0x69, 0x73, 0x74, 0x72, 0x79, 0x12, 0x1c, 0x0a, 0x09, 0x6e, 0x61, 0x6d, 0x65, 0x73,
	0x70, 0x61, 0x63, 0x65, 0x18, 0x01, 0x20, 0x01, 0x28, 0x09, 0x52, 0x09, 0x6e, 0x61, 0x6d, 0x65,
	0x73, 0x70, 0x61, 0x63, 0x65, 0x12, 0x27, 0x0a, 0x04, 0x65, 0x74, 0x63, 0x64, 0x18, 0x02, 0x20,
	0x01, 0x28, 0x0b, 0x32, 0x13, 0x2e, 0x63, 0x6f, 0x6e, 0x66, 0x2e, 0x52, 0x65, 0x67, 0x69, 0x73,
	0x74, 0x72, 0x79, 0x2e, 0x45, 0x74, 0x63, 0x64, 0x52, 0x04, 0x65, 0x74, 0x63, 0x64, 0x1a, 0x23,
	0x0a, 0x04, 0x45, 0x74, 0x63, 0x64, 0x12, 0x1b, 0x0a, 0x09, 0x6d, 0x61, 0x78, 0x5f, 0x72, 0x65,
	0x74, 0x72, 0x79, 0x18, 0x01, 0x20, 0x01, 0x28, 0x05, 0x52, 0x08, 0x6d, 0x61, 0x78, 0x52, 0x65,
	0x74, 0x72, 0x79, 0x22, 0xef, 0x07, 0x0a, 0x04, 0x44, 0x61, 0x74, 0x61, 0x12, 0x26, 0x0a, 0x05,
	0x72, 0x65, 0x64, 0x69, 0x73, 0x18, 0x01, 0x20, 0x01, 0x28, 0x0b, 0x32, 0x10, 0x2e, 0x63, 0x6f,
	0x6e, 0x66, 0x2e, 0x44, 0x61, 0x74, 0x61, 0x2e, 0x52, 0x65, 0x64, 0x69, 0x73, 0x52, 0x05, 0x72,
	0x65, 0x64, 0x69, 0x73, 0x12, 0x23, 0x0a, 0x04, 0x65, 0x74, 0x63, 0x64, 0x18, 0x02, 0x20, 0x01,
	0x28, 0x0b, 0x32, 0x0f, 0x2e, 0x63, 0x6f, 0x6e, 0x66, 0x2e, 0x44, 0x61, 0x74, 0x61, 0x2e, 0x45,
	0x74, 0x63, 0x64, 0x52, 0x04, 0x65, 0x74, 0x63, 0x64, 0x12, 0x26, 0x0a, 0x05, 0x6b, 0x61, 0x66,
	0x6b, 0x61, 0x18, 0x03, 0x20, 0x01, 0x28, 0x0b, 0x32, 0x10, 0x2e, 0x63, 0x6f, 0x6e, 0x66, 0x2e,
	0x44, 0x61, 0x74, 0x61, 0x2e, 0x4b, 0x61, 0x66, 0x6b, 0x61, 0x52, 0x05, 0x6b, 0x61, 0x66, 0x6b,
	0x61, 0x12, 0x29, 0x0a, 0x06, 0x74, 0x72, 0x61, 0x63, 0x65, 0x72, 0x18, 0x04, 0x20, 0x01, 0x28,
	0x0b, 0x32, 0x11, 0x2e, 0x63, 0x6f, 0x6e, 0x66, 0x2e, 0x44, 0x61, 0x74, 0x61, 0x2e, 0x54, 0x72,
	0x61, 0x63, 0x65, 0x72, 0x52, 0x06, 0x74, 0x72, 0x61, 0x63, 0x65, 0x72, 0x1a, 0xac, 0x03, 0x0a,
	0x05, 0x52, 0x65, 0x64, 0x69, 0x73, 0x12, 0x18, 0x0a, 0x07, 0x70, 0x72, 0x69, 0x6d, 0x61, 0x72,
	0x79, 0x18, 0x01, 0x20, 0x01, 0x28, 0x09, 0x52, 0x07, 0x70, 0x72, 0x69, 0x6d, 0x61, 0x72, 0x79,
	0x12, 0x12, 0x0a, 0x04, 0x68, 0x6f, 0x73, 0x74, 0x18, 0x02, 0x20, 0x01, 0x28, 0x09, 0x52, 0x04,
	0x68, 0x6f, 0x73, 0x74, 0x12, 0x12, 0x0a, 0x04, 0x70, 0x6f, 0x72, 0x74, 0x18, 0x03, 0x20, 0x01,
	0x28, 0x04, 0x52, 0x04, 0x70, 0x6f, 0x72, 0x74, 0x12, 0x1a, 0x0a, 0x08, 0x70, 0x61, 0x73, 0x73,
	0x77, 0x6f, 0x72, 0x64, 0x18, 0x04, 0x20, 0x01, 0x28, 0x09, 0x52, 0x08, 0x70, 0x61, 0x73, 0x73,
	0x77, 0x6f, 0x72, 0x64, 0x12, 0x0e, 0x0a, 0x02, 0x64, 0x62, 0x18, 0x05, 0x20, 0x01, 0x28, 0x04,
	0x52, 0x02, 0x64, 0x62, 0x12, 0x1a, 0x0a, 0x08, 0x75, 0x73, 0x65, 0x72, 0x6e, 0x61, 0x6d, 0x65,
	0x18, 0x06, 0x20, 0x01, 0x28, 0x09, 0x52, 0x08, 0x75, 0x73, 0x65, 0x72, 0x6e, 0x61, 0x6d, 0x65,
	0x12, 0x2b, 0x0a, 0x11, 0x73, 0x65, 0x6e, 0x74, 0x69, 0x6e, 0x65, 0x6c, 0x5f, 0x75, 0x73, 0x65,
	0x72, 0x6e, 0x61, 0x6d, 0x65, 0x18, 0x07, 0x20, 0x01, 0x28, 0x09, 0x52, 0x10, 0x73, 0x65, 0x6e,
	0x74, 0x69, 0x6e, 0x65, 0x6c, 0x55, 0x73, 0x65, 0x72, 0x6e, 0x61, 0x6d, 0x65, 0x12, 0x2b, 0x0a,
	0x11, 0x73, 0x65, 0x6e, 0x74, 0x69, 0x6e, 0x65, 0x6c, 0x5f, 0x70, 0x61, 0x73, 0x73, 0x77, 0x6f,
	0x72, 0x64, 0x18, 0x08, 0x20, 0x01, 0x28, 0x09, 0x52, 0x10, 0x73, 0x65, 0x6e, 0x74, 0x69, 0x6e,
	0x65, 0x6c, 0x50, 0x61, 0x73, 0x73, 0x77, 0x6f, 0x72, 0x64, 0x12, 0x1b, 0x0a, 0x09, 0x70, 0x6f,
	0x6f, 0x6c, 0x5f, 0x73, 0x69, 0x7a, 0x65, 0x18, 0x09, 0x20, 0x01, 0x28, 0x04, 0x52, 0x08, 0x70,
	0x6f, 0x6f, 0x6c, 0x53, 0x69, 0x7a, 0x65, 0x12, 0x24, 0x0a, 0x0e, 0x6d, 0x69, 0x6e, 0x5f, 0x69,
	0x64, 0x6c, 0x65, 0x5f, 0x63, 0x6f, 0x6e, 0x6e, 0x73, 0x18, 0x0a, 0x20, 0x01, 0x28, 0x04, 0x52,
	0x0c, 0x6d, 0x69, 0x6e, 0x49, 0x64, 0x6c, 0x65, 0x43, 0x6f, 0x6e, 0x6e, 0x73, 0x12, 0x3c, 0x0a,
	0x0c, 0x72, 0x65, 0x61, 0x64, 0x5f, 0x74, 0x69, 0x6d, 0x65, 0x6f, 0x75, 0x74, 0x18, 0x0b, 0x20,
	0x01, 0x28, 0x0b, 0x32, 0x19, 0x2e, 0x67, 0x6f, 0x6f, 0x67, 0x6c, 0x65, 0x2e, 0x70, 0x72, 0x6f,
	0x74, 0x6f, 0x62, 0x75, 0x66, 0x2e, 0x44, 0x75, 0x72, 0x61, 0x74, 0x69, 0x6f, 0x6e, 0x52, 0x0b,
	0x72, 0x65, 0x61, 0x64, 0x54, 0x69, 0x6d, 0x65, 0x6f, 0x75, 0x74, 0x12, 0x3e, 0x0a, 0x0d, 0x77,
	0x72, 0x69, 0x74, 0x65, 0x5f, 0x74, 0x69, 0x6d, 0x65, 0x6f, 0x75, 0x74, 0x18, 0x0c, 0x20, 0x01,
	0x28, 0x0b, 0x32, 0x19, 0x2e, 0x67, 0x6f, 0x6f, 0x67, 0x6c, 0x65, 0x2e, 0x70, 0x72, 0x6f, 0x74,
	0x6f, 0x62, 0x75, 0x66, 0x2e, 0x44, 0x75, 0x72, 0x61, 0x74, 0x69, 0x6f, 0x6e, 0x52, 0x0c, 0x77,
	0x72, 0x69, 0x74, 0x65, 0x54, 0x69, 0x6d, 0x65, 0x6f, 0x75, 0x74, 0x1a, 0x66, 0x0a, 0x04, 0x45,
	0x74, 0x63, 0x64, 0x12, 0x12, 0x0a, 0x04, 0x68, 0x6f, 0x73, 0x74, 0x18, 0x01, 0x20, 0x01, 0x28,
	0x09, 0x52, 0x04, 0x68, 0x6f, 0x73, 0x74, 0x12, 0x12, 0x0a, 0x04, 0x70, 0x6f, 0x72, 0x74, 0x18,
	0x02, 0x20, 0x01, 0x28, 0x04, 0x52, 0x04, 0x70, 0x6f, 0x72, 0x74, 0x12, 0x1a, 0x0a, 0x08, 0x75,
	0x73, 0x65, 0x72, 0x6e, 0x61, 0x6d, 0x65, 0x18, 0x03, 0x20, 0x01, 0x28, 0x09, 0x52, 0x08, 0x75,
	0x73, 0x65, 0x72, 0x6e, 0x61, 0x6d, 0x65, 0x12, 0x1a, 0x0a, 0x08, 0x70, 0x61, 0x73, 0x73, 0x77,
	0x6f, 0x72, 0x64, 0x18, 0x04, 0x20, 0x01, 0x28, 0x09, 0x52, 0x08, 0x70, 0x61, 0x73, 0x73, 0x77,
	0x6f, 0x72, 0x64, 0x1a, 0xd5, 0x01, 0x0a, 0x05, 0x4b, 0x61, 0x66, 0x6b, 0x61, 0x12, 0x12, 0x0a,
	0x04, 0x68, 0x6f, 0x73, 0x74, 0x18, 0x01, 0x20, 0x01, 0x28, 0x09, 0x52, 0x04, 0x68, 0x6f, 0x73,
	0x74, 0x12, 0x12, 0x0a, 0x04, 0x70, 0x6f, 0x72, 0x74, 0x18, 0x02, 0x20, 0x01, 0x28, 0x04, 0x52,
	0x04, 0x70, 0x6f, 0x72, 0x74, 0x12, 0x1a, 0x0a, 0x08, 0x75, 0x73, 0x65, 0x72, 0x6e, 0x61, 0x6d,
	0x65, 0x18, 0x03, 0x20, 0x01, 0x28, 0x09, 0x52, 0x08, 0x75, 0x73, 0x65, 0x72, 0x6e, 0x61, 0x6d,
	0x65, 0x12, 0x1a, 0x0a, 0x08, 0x70, 0x61, 0x73, 0x73, 0x77, 0x6f, 0x72, 0x64, 0x18, 0x04, 0x20,
	0x01, 0x28, 0x09, 0x52, 0x08, 0x70, 0x61, 0x73, 0x73, 0x77, 0x6f, 0x72, 0x64, 0x12, 0x23, 0x0a,
	0x0d, 0x70, 0x72, 0x6f, 0x64, 0x75, 0x63, 0x65, 0x72, 0x5f, 0x6d, 0x6f, 0x64, 0x65, 0x18, 0x05,
	0x20, 0x01, 0x28, 0x09, 0x52, 0x0c, 0x70, 0x72, 0x6f, 0x64, 0x75, 0x63, 0x65, 0x72, 0x4d, 0x6f,
	0x64, 0x65, 0x12, 0x18, 0x0a, 0x07, 0x74, 0x69, 0x6d, 0x65, 0x6f, 0x75, 0x74, 0x18, 0x06, 0x20,
	0x01, 0x28, 0x04, 0x52, 0x07, 0x74, 0x69, 0x6d, 0x65, 0x6f, 0x75, 0x74, 0x12, 0x2d, 0x0a, 0x12,
	0x6d, 0x61, 0x6e, 0x75, 0x61, 0x6c, 0x5f, 0x70, 0x61, 0x72, 0x74, 0x69, 0x74, 0x69, 0x6f, 0x6e,
	0x65, 0x72, 0x18, 0x07, 0x20, 0x01, 0x28, 0x09, 0x52, 0x11, 0x6d, 0x61, 0x6e, 0x75, 0x61, 0x6c,
	0x50, 0x61, 0x72, 0x74, 0x69, 0x74, 0x69, 0x6f, 0x6e, 0x65, 0x72, 0x1a, 0x58, 0x0a, 0x06, 0x54,
	0x72, 0x61, 0x63, 0x65, 0x72, 0x12, 0x10, 0x0a, 0x03, 0x75, 0x72, 0x6c, 0x18, 0x01, 0x20, 0x01,
	0x28, 0x09, 0x52, 0x03, 0x75, 0x72, 0x6c, 0x12, 0x1d, 0x0a, 0x0a, 0x61, 0x67, 0x65, 0x6e, 0x74,
	0x5f, 0x68, 0x6f, 0x73, 0x74, 0x18, 0x02, 0x20, 0x01, 0x28, 0x09, 0x52, 0x09, 0x61, 0x67, 0x65,
	0x6e, 0x74, 0x48, 0x6f, 0x73, 0x74, 0x12, 0x1d, 0x0a, 0x0a, 0x61, 0x67, 0x65, 0x6e, 0x74, 0x5f,
	0x70, 0x6f, 0x72, 0x74, 0x18, 0x03, 0x20, 0x01, 0x28, 0x09, 0x52, 0x09, 0x61, 0x67, 0x65, 0x6e,
	0x74, 0x50, 0x6f, 0x72, 0x74, 0x42, 0x09, 0x5a, 0x07, 0x2e, 0x2f, 0x3b, 0x63, 0x6f, 0x6e, 0x66,
	0x62, 0x06, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x33,
}

var (
	file_common_proto_rawDescOnce sync.Once
	file_common_proto_rawDescData = file_common_proto_rawDesc
)

func file_common_proto_rawDescGZIP() []byte {
	file_common_proto_rawDescOnce.Do(func() {
		file_common_proto_rawDescData = protoimpl.X.CompressGZIP(file_common_proto_rawDescData)
	})
	return file_common_proto_rawDescData
}

var file_common_proto_msgTypes = make([]protoimpl.MessageInfo, 10)
var file_common_proto_goTypes = []any{
	(*Bootstrap)(nil),           // 0: conf.Bootstrap
	(*ConfigCenter)(nil),        // 1: conf.ConfigCenter
	(*Registry)(nil),            // 2: conf.Registry
	(*Data)(nil),                // 3: conf.Data
	(*ConfigCenter_Nacos)(nil),  // 4: conf.ConfigCenter.Nacos
	(*Registry_Etcd)(nil),       // 5: conf.Registry.Etcd
	(*Data_Redis)(nil),          // 6: conf.Data.Redis
	(*Data_Etcd)(nil),           // 7: conf.Data.Etcd
	(*Data_Kafka)(nil),          // 8: conf.Data.Kafka
	(*Data_Tracer)(nil),         // 9: conf.Data.Tracer
	(*durationpb.Duration)(nil), // 10: google.protobuf.Duration
}
var file_common_proto_depIdxs = []int32{
	1,  // 0: conf.Bootstrap.config_center:type_name -> conf.ConfigCenter
	2,  // 1: conf.Bootstrap.registry:type_name -> conf.Registry
	3,  // 2: conf.Bootstrap.data:type_name -> conf.Data
	4,  // 3: conf.ConfigCenter.nacos:type_name -> conf.ConfigCenter.Nacos
	5,  // 4: conf.Registry.etcd:type_name -> conf.Registry.Etcd
	6,  // 5: conf.Data.redis:type_name -> conf.Data.Redis
	7,  // 6: conf.Data.etcd:type_name -> conf.Data.Etcd
	8,  // 7: conf.Data.kafka:type_name -> conf.Data.Kafka
	9,  // 8: conf.Data.tracer:type_name -> conf.Data.Tracer
	10, // 9: conf.Data.Redis.read_timeout:type_name -> google.protobuf.Duration
	10, // 10: conf.Data.Redis.write_timeout:type_name -> google.protobuf.Duration
	11, // [11:11] is the sub-list for method output_type
	11, // [11:11] is the sub-list for method input_type
	11, // [11:11] is the sub-list for extension type_name
	11, // [11:11] is the sub-list for extension extendee
	0,  // [0:11] is the sub-list for field type_name
}

func init() { file_common_proto_init() }
func file_common_proto_init() {
	if File_common_proto != nil {
		return
	}
	if !protoimpl.UnsafeEnabled {
		file_common_proto_msgTypes[0].Exporter = func(v any, i int) any {
			switch v := v.(*Bootstrap); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_common_proto_msgTypes[1].Exporter = func(v any, i int) any {
			switch v := v.(*ConfigCenter); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_common_proto_msgTypes[2].Exporter = func(v any, i int) any {
			switch v := v.(*Registry); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_common_proto_msgTypes[3].Exporter = func(v any, i int) any {
			switch v := v.(*Data); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_common_proto_msgTypes[4].Exporter = func(v any, i int) any {
			switch v := v.(*ConfigCenter_Nacos); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_common_proto_msgTypes[5].Exporter = func(v any, i int) any {
			switch v := v.(*Registry_Etcd); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_common_proto_msgTypes[6].Exporter = func(v any, i int) any {
			switch v := v.(*Data_Redis); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_common_proto_msgTypes[7].Exporter = func(v any, i int) any {
			switch v := v.(*Data_Etcd); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_common_proto_msgTypes[8].Exporter = func(v any, i int) any {
			switch v := v.(*Data_Kafka); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_common_proto_msgTypes[9].Exporter = func(v any, i int) any {
			switch v := v.(*Data_Tracer); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
	}
	type x struct{}
	out := protoimpl.TypeBuilder{
		File: protoimpl.DescBuilder{
			GoPackagePath: reflect.TypeOf(x{}).PkgPath(),
			RawDescriptor: file_common_proto_rawDesc,
			NumEnums:      0,
			NumMessages:   10,
			NumExtensions: 0,
			NumServices:   0,
		},
		GoTypes:           file_common_proto_goTypes,
		DependencyIndexes: file_common_proto_depIdxs,
		MessageInfos:      file_common_proto_msgTypes,
	}.Build()
	File_common_proto = out.File
	file_common_proto_rawDesc = nil
	file_common_proto_goTypes = nil
	file_common_proto_depIdxs = nil
}