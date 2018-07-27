package cmqapi

import (
	"github.com/friendlyhank/foundation/str"
)

//CmqQueue -队列
type CmqQueue struct {
	QueueName string //队列名
	CmqClient *CmqClient
	Encoding  bool //64位编码
}

//NewCmqQueue -
func (q *CmqQueue) NewCmqQueue(queuename string, cmqclient *CmqClient, encoding bool) *CmqQueue {
	return &CmqQueue{QueueName: queuename, CmqClient: cmqclient, Encoding: encoding}
}

//===============================================queue operation===============================================

/**
*Create
*创建队列
@type queuemeta: QueueMeta struct
@return
**/
func (q *CmqQueue) Create(queuemeta *QueueMeta) (err error) {
	//此处默认
	params := map[string]string{
		"queueName":           queuemeta.Queuename,
		"pollingWaitSeconds":  str.Int642str(queuemeta.PollingWaitSeconds),
		"visibilityTimeout":   str.Int642str(queuemeta.VisibilityTimeout),
		"maxMsgSize":          str.Int642str(queuemeta.MaxMsgSize),
		"msgRetentionSeconds": str.Int642str(queuemeta.MsgRetentionSeconds),
		"rewindSeconds":       str.Int642str(queuemeta.RewindSeconds),
	}
	_, err = q.CmqClient.CreateQueue(params)
	return
}

//===============================================QueueMeta operation===============================================

/*
QueueMeta - 队列属性
#note: 设置属性
MaxMsgHeapNum：最大堆积消息数量
PollingWaitSeconds：消息接收长轮询等待时间.取值范围0-30 单位：秒
VisibilityTimeout：消息可见性超时 单位：秒
MaxMsgSize：消息的最大长度 单位：秒
MsgRetentionSeconds：消息保留周期 单位：秒
RewindSeconds：队列是否开启消息回溯能力 最大回溯时间 单位：秒

*/
type QueueMeta struct {
	MaxMsgHeapNum       int64
	PollingWaitSeconds  int64
	VisibilityTimeout   int64
	MaxMsgSize          int64
	MsgRetentionSeconds int64
	RewindSeconds       int64
	Queuename           string
}

//===============================================message operation===============================================

//Message -
type Message struct {
	MsgBody          string //消费的消息正文
	MsgID            string //消费的消息唯一标识ID
	ReceiptHandle    string //每次消费返回唯一的消息句柄，用于删除消费。仅上一次消费该消息产生的句柄能用于删除消息
	EnqueueTime      int64  //消费被生产出来，进入队列的时间。
	FirstDequeueTime int64  //第一次消费该消息的时间。
	NextVisibleTime  int64  //消息的下次可见时间
	DequeueCount     int64  //消息被消费次数
}

/*
*SendMessage
*发送消息
@param queuename string
@param message struct
@param delaytime int64
@return
*/
func (q *CmqQueue) SendMessage(queuename string, message *Message, delaytime int64) (msgid string, err error) {
	params := map[string]string{
		"queueName":    queuename,
		"msgBody":      message.MsgBody,
		"delaySeconds": str.Int642str(delaytime),
	}
	msgid, err = q.CmqClient.SendMessage(params)
	return
}

//ReceiveMessage -消费消息
/*
@type pollingwaitseconds: int
@param pollingwaitseconds: 本次请求的长轮询时间，单位: 秒

@rtype Message object
@return Message object 中包含基本属性、 临时句柄
*/
func (q *CmqQueue) ReceiveMessage(queuename string, pollingwaitseconds int64) (msg *Message, err error) {
	params := map[string]string{
		"queueName": queuename,
	}
	if 0 != pollingwaitseconds {
		params["UserpollingWaitSeconds"] = str.Int642str(pollingwaitseconds)
		params["pollingWaitSeconds"] = str.Int642str(pollingwaitseconds)
	} else {
		params["UserpollingWaitSeconds"] = str.Int642str(30)
	}

	resp := &ReceiveMessageRes{}
	resp, err = q.CmqClient.ReceiveMessage(params)

	msg = new(Message)
	msg.MsgID = resp.MsgID
	msg.ReceiptHandle = resp.ReceiptHandle
	msg.EnqueueTime = resp.EnqueueTime
	msg.NextVisibleTime = resp.NextVisibleTime
	msg.DequeueCount = resp.DequeueCount
	msg.FirstDequeueTime = resp.FirstDequeueTime
	return
}
