package rpc

import (
	"github.com/njtc406/server_engine/util/sync"
	"reflect"
	"time"
)

type RpcRequest struct {
	ref            bool            //数据是否正在被使用
	RpcRequestData IRpcRequestData //请求的数据信息

	inParam    interface{} //参数
	localReply interface{}

	requestHandle RequestHandler //请求的处理函数
	callback      *reflect.Value //回调
	rpcProcessor  IRpcProcessor  //消息处理器
}

type RpcResponse struct {
	RpcResponseData IRpcResponseData //回复的数据信息
}

type Responder = RequestHandler

func (r *Responder) IsInvalid() bool {
	return reflect.ValueOf(*r).Pointer() == reflect.ValueOf(reqHandlerNull).Pointer()
}

// rpc req对象池(用于生成请求对象)
var rpcRequestPool = sync.NewPoolEx(make(chan sync.IPoolData, 10240), func() sync.IPoolData {
	return &RpcRequest{}
})

// rpc call对象池
var rpcCallPool = sync.NewPoolEx(make(chan sync.IPoolData, 10240), func() sync.IPoolData {
	return &Call{done: make(chan *Call, 1)}
})

type IRpcRequestData interface {
	GetSeq() uint64
	GetServiceMethod() string
	GetInParam() []byte
	IsNoReply() bool
	GetRpcMethodId() uint32
}

type IRpcResponseData interface {
	GetSeq() uint64
	GetErr() *RpcError
	GetReply() []byte //获取回复信息
}

type IRawInputArgs interface {
	GetRawData() []byte //获取原始数据
	DoFree()            //处理完成,回收内存
	DoEscape()          //逃逸,GC自动回收
}

type RpcHandleFinder interface {
	FindRpcHandler(serviceMethod string) IRpcHandler
}

type RequestHandler func(Returns interface{}, Err RpcError)

type Call struct {
	ref           bool
	Seq           uint64
	ServiceMethod string
	Reply         interface{}  //是否需要回复
	Response      *RpcResponse //回复信息
	Err           error
	done          chan *Call // 当call完成时的通知通道
	connId        int
	callback      *reflect.Value
	rpcHandler    IRpcHandler
	callTime      time.Time //开始call的时间
}

func (slf *RpcRequest) Clear() *RpcRequest {
	slf.RpcRequestData = nil
	slf.localReply = nil
	slf.inParam = nil
	slf.requestHandle = nil
	slf.callback = nil
	slf.rpcProcessor = nil
	return slf
}

func (slf *RpcRequest) Reset() {
	slf.Clear()
}

func (slf *RpcRequest) IsRef() bool {
	return slf.ref
}

func (slf *RpcRequest) Ref() {
	slf.ref = true
}

func (slf *RpcRequest) UnRef() {
	slf.ref = false
}

func (rpcResponse *RpcResponse) Clear() *RpcResponse {
	rpcResponse.RpcResponseData = nil
	return rpcResponse
}

func (call *Call) Clear() *Call {
	call.Seq = 0
	call.ServiceMethod = ""
	call.Reply = nil
	call.Response = nil
	if len(call.done) > 0 {
		call.done = make(chan *Call, 1)
	}

	call.Err = nil
	call.connId = 0
	call.callback = nil
	call.rpcHandler = nil
	return call
}

func (call *Call) Reset() {
	call.Clear()
}

func (call *Call) IsRef() bool {
	return call.ref
}

func (call *Call) Ref() {
	call.ref = true
}

func (call *Call) UnRef() {
	call.ref = false
}

func (call *Call) Done() *Call {
	return <-call.done
}

func MakeRpcRequest(rpcProcessor IRpcProcessor, seq uint64, rpcMethodId uint32, serviceMethod string, noReply bool, inParam []byte) *RpcRequest {
	rpcRequest := rpcRequestPool.Get().(*RpcRequest)
	rpcRequest.rpcProcessor = rpcProcessor
	rpcRequest.RpcRequestData = rpcRequest.rpcProcessor.MakeRpcRequest(seq, rpcMethodId, serviceMethod, noReply, inParam)

	return rpcRequest
}

func ReleaseRpcRequest(rpcRequest *RpcRequest) {
	rpcRequest.rpcProcessor.ReleaseRpcRequest(rpcRequest.RpcRequestData)
	rpcRequestPool.Put(rpcRequest)
}

func MakeCall() *Call {
	return rpcCallPool.Get().(*Call)
}

func ReleaseCall(call *Call) {
	rpcCallPool.Put(call)
}
