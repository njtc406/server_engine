package rpc

import (
	"container/list"
	"errors"
	"fmt"
	"github.com/njtc406/server_engine/cluster"
	"github.com/njtc406/server_engine/log"
	"github.com/njtc406/server_engine/network"
	"math"
	"reflect"
	"runtime"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

type Client struct {
	clientSeq         uint32
	id                int
	bSelfNode         bool
	network.TCPClient //连接类型,默认使用tcp
	conn              *network.TCPConn

	pendingLock          sync.RWMutex //等待队列的锁
	startSeq             uint64       //起始序号
	pending              map[uint64]*list.Element
	pendingTimer         *list.List    //等待队列
	callRpcTimeout       time.Duration //超时时间
	maxCheckCallRpcCount int           //回调超时时间
	TriggerRpcEvent                    //事件触发
}

// 回调超时时间
const MaxCheckCallRpcCount = 1000

// 写入队列容量
const MaxPendingWriteNum = 200000

// 重连间隔
const ConnectInterval = 2 * time.Second

// 自增量
var clientSeq uint32

// 重置连接
func (client *Client) NewClientAgent(conn *network.TCPConn) network.Agent {
	client.conn = conn
	client.ResetPending()

	return client
}

func (client *Client) Connect(id int, addr string, maxRpcParamLen uint32) error {
	client.clientSeq = atomic.AddUint32(&clientSeq, 1)
	client.id = id
	client.Addr = addr
	client.maxCheckCallRpcCount = MaxCheckCallRpcCount
	client.callRpcTimeout = 15 * time.Second
	client.ConnectInterval = ConnectInterval
	client.PendingWriteNum = MaxPendingWriteNum
	client.AutoReconnect = true

	client.ConnNum = 1
	client.LenMsgLen = 4
	client.MinMsgLen = 2
	client.ReadDeadline = Default_ReadWriteDeadline
	client.WriteDeadline = Default_ReadWriteDeadline

	if maxRpcParamLen > 0 {
		client.MaxMsgLen = maxRpcParamLen
	} else {
		client.MaxMsgLen = math.MaxUint32
	}

	client.NewAgent = client.NewClientAgent
	client.LittleEndian = LittleEndian
	client.ResetPending()
	go client.startCheckRpcCallTimer()
	if addr == "" {
		client.bSelfNode = true
		return nil
	}

	client.Start()
	return nil
}

// 每5秒检查一次是否有回调超时,如果超时,则向回调回复callFail
func (client *Client) startCheckRpcCallTimer() {
	for {
		time.Sleep(5 * time.Second)
		client.checkRpcCallTimeout()
	}
}

// 回复失败
func (client *Client) makeCallFail(call *Call) {
	//从等待回复队列移除
	client.removePending(call.Seq)
	if call.callback != nil && call.callback.IsValid() {
		call.rpcHandler.PushRpcResponse(call)
	} else {
		call.done <- call
	}
}

// 检查rpc调用是否超时
func (client *Client) checkRpcCallTimeout() {
	now := time.Now()

	for i := 0; i < client.maxCheckCallRpcCount; i++ {
		client.pendingLock.Lock()
		pElem := client.pendingTimer.Front()
		if pElem == nil {
			client.pendingLock.Unlock()
			break
		}
		pCall := pElem.Value.(*Call)
		if now.Sub(pCall.callTime) > client.callRpcTimeout {
			strTimeout := strconv.FormatInt(int64(client.callRpcTimeout/time.Second), 10)
			pCall.Err = errors.New("RPC call takes more than " + strTimeout + " seconds")
			client.makeCallFail(pCall)
			client.pendingLock.Unlock()
			continue
		}
		client.pendingLock.Unlock()
	}
}

// 重置等待队列
func (client *Client) ResetPending() {
	client.pendingLock.Lock()
	if client.pending != nil {
		for _, v := range client.pending {
			v.Value.(*Call).Err = errors.New("node is disconnect")
			v.Value.(*Call).done <- v.Value.(*Call)
		}
	}

	client.pending = make(map[uint64]*list.Element, 4096)
	client.pendingTimer = list.New()
	client.pendingLock.Unlock()
}

// 加入等待队列
func (client *Client) AddPending(call *Call) {
	client.pendingLock.Lock()
	call.callTime = time.Now()
	elemTimer := client.pendingTimer.PushBack(call)
	client.pending[call.Seq] = elemTimer //如果下面发送失败，将会一一直存在这里
	client.pendingLock.Unlock()
}

// 从等待队列移除
func (client *Client) RemovePending(seq uint64) *Call {
	if seq == 0 {
		return nil
	}
	client.pendingLock.Lock()
	call := client.removePending(seq)
	client.pendingLock.Unlock()
	return call
}

func (client *Client) removePending(seq uint64) *Call {
	v, ok := client.pending[seq]
	if ok == false {
		return nil
	}
	call := v.Value.(*Call)
	client.pendingTimer.Remove(v)
	delete(client.pending, seq)
	return call
}

// 获取等待队列中的某个处理对象
func (client *Client) FindPending(seq uint64) *Call {
	if seq == 0 {
		return nil
	}

	client.pendingLock.Lock()
	v, ok := client.pending[seq]
	if ok == false {
		client.pendingLock.Unlock()
		return nil
	}

	pCall := v.Value.(*Call)
	client.pendingLock.Unlock()

	return pCall
}

// 生成自增长序列
func (client *Client) generateSeq() uint64 {
	return atomic.AddUint64(&client.startSeq, 1)
}

func (client *Client) AsyncCall(rpcHandler IRpcHandler, serviceMethod string, callback reflect.Value, args interface{}, replyParam interface{}) error {
	processorType, processor := GetProcessorType(args)
	InParam, herr := processor.Marshal(args)
	if herr != nil {
		return herr
	}

	seq := client.generateSeq()
	request := MakeRpcRequest(processor, seq, 0, serviceMethod, false, InParam)
	bytes, err := processor.Marshal(request.RpcRequestData)
	ReleaseRpcRequest(request)
	if err != nil {
		return err
	}

	if client.conn == nil {
		return errors.New("Rpc server is disconnect,call " + serviceMethod)
	}

	call := MakeCall()
	call.Reply = replyParam
	call.callback = &callback
	call.rpcHandler = rpcHandler
	call.ServiceMethod = serviceMethod
	call.Seq = seq
	client.AddPending(call)

	err = client.conn.WriteMsg([]byte{uint8(processorType)}, bytes)
	if err != nil {
		client.RemovePending(call.Seq)
		ReleaseCall(call)
		return err
	}

	return nil
}

// 数据发送
func (client *Client) RawGo(processor IRpcProcessor, noReply bool, rpcMethodId uint32, serviceMethod string, args []byte, reply interface{}) *Call {
	//从callPool中获取一个call对象
	call := MakeCall()
	call.ServiceMethod = serviceMethod
	call.Reply = reply
	call.Seq = client.generateSeq()

	//生成rpc请求对象
	request := MakeRpcRequest(processor, call.Seq, rpcMethodId, serviceMethod, noReply, args)
	//序列化
	bytes, err := processor.Marshal(request.RpcRequestData)
	//释放请求对象
	ReleaseRpcRequest(request)
	// err在这里判断是为了防止request对象泄漏
	if err != nil {
		call.Seq = 0
		call.Err = err
		return call
	}

	if client.conn == nil {
		call.Seq = 0
		call.Err = errors.New(serviceMethod + "  was called failed,rpc client is disconnect")
		return call
	}

	if noReply == false {
		//如果是需要回复,加入等待列表
		client.AddPending(call)
	}

	err = client.conn.WriteMsg([]byte{uint8(processor.GetProcessorType())}, bytes)
	if err != nil {
		client.RemovePending(call.Seq)
		call.Seq = 0
		call.Err = err
	}

	return call
}

func (client *Client) Go(noReply bool, serviceMethod string, args interface{}, reply interface{}) *Call {
	_, processor := GetProcessorType(args)
	InParam, err := processor.Marshal(args)
	if err != nil {
		call := MakeCall()
		call.Err = err
		return call
	}

	return client.RawGo(processor, noReply, 0, serviceMethod, InParam, reply)
}

// 执行一个rpc请求
func (client *Client) Run() {
	defer func() {
		//异常捕获
		if r := recover(); r != nil {
			buf := make([]byte, 4096)
			l := runtime.Stack(buf, false)
			errString := fmt.Sprint(r)
			log.SError("core dump info[", errString, "]\n", string(buf[:l]))
		}
	}()

	//将请求加入rpc事件队列,等待执行
	client.TriggerRpcEvent(true, client.GetClientSeq(), client.GetId(), cluster.GetCluster().GetVersion())
	for {
		//等待回复
		bytes, err := client.conn.ReadMsg()
		if err != nil {
			log.SError("rpcClient ", client.Addr, " ReadMsg error:", err.Error())
			return
		}

		//先读取消息头
		processor := GetProcessor(bytes[0])
		if processor == nil {
			//消息头错误,表示是异常消息,直接抛弃
			client.conn.ReleaseReadMsg(bytes)
			log.SError("rpcClient ", client.Addr, " ReadMsg head error:", err.Error())
			return
		}

		//1.解析head
		response := RpcResponse{}
		response.RpcResponseData = processor.MakeRpcResponse(0, "", nil)

		//解析消息体
		err = processor.Unmarshal(bytes[1:], response.RpcResponseData)
		client.conn.ReleaseReadMsg(bytes)
		if err != nil {
			processor.ReleaseRpcResponse(response.RpcResponseData)
			log.SError("rpcClient Unmarshal head error:", err.Error())
			continue
		}

		v := client.RemovePending(response.RpcResponseData.GetSeq())
		if v == nil {
			log.SError("rpcClient cannot find seq ", response.RpcResponseData.GetSeq(), " in pending")
		} else {
			v.Err = nil
			if len(response.RpcResponseData.GetReply()) > 0 {
				err = processor.Unmarshal(response.RpcResponseData.GetReply(), v.Reply)
				if err != nil {
					log.SError("rpcClient Unmarshal body error:", err.Error())
					v.Err = err
				}
			}

			if response.RpcResponseData.GetErr() != nil {
				v.Err = response.RpcResponseData.GetErr()
			}

			if v.callback != nil && v.callback.IsValid() {
				v.rpcHandler.PushRpcResponse(v)
			} else {
				v.done <- v
			}
		}

		processor.ReleaseRpcResponse(response.RpcResponseData)
	}
}

func (client *Client) OnClose() {
	client.TriggerRpcEvent(false, client.GetClientSeq(), client.GetId(), cluster.GetCluster().GetVersion())
}

func (client *Client) IsConnected() bool {
	return client.bSelfNode || (client.conn != nil && client.conn.IsConnected() == true)
}

func (client *Client) GetId() int {
	return client.id
}

func (client *Client) Close(waitDone bool) {
	client.TCPClient.Close(waitDone)

	client.pendingLock.Lock()
	for {
		pElem := client.pendingTimer.Front()
		if pElem == nil {
			break
		}

		pCall := pElem.Value.(*Call)
		pCall.Err = errors.New("nodeid is disconnect ")
		client.makeCallFail(pCall)
	}
	client.pendingLock.Unlock()
}

func (client *Client) GetClientSeq() uint32 {
	return client.clientSeq
}
