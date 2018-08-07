package cmqapi

import (
	"fmt"

	"github.com/friendlyhank/foundation/str"
)

//CmqQueue -队列
type CmqQueue struct {
	QueueName string //队列名
	CmqClient *CmqClient
	Encoding  bool //64位编码
}

//NewCmqQueue -
func NewCmqQueue(queuename string, cmqclient *CmqClient, encoding bool) *CmqQueue {
	return &CmqQueue{QueueName: queuename, CmqClient: cmqclient, Encoding: encoding}
}

//===============================================queue operation===============================================

/**
*Create
*创建队列
@type queuemeta: QueueMeta struct
@return err
**/
func (q *CmqQueue) Create(queuemeta *QueueMeta) (err error) {
	//此处默认
	params := map[string]string{
		"queueName":           q.QueueName,
		"pollingWaitSeconds":  str.Int642str(queuemeta.PollingWaitSeconds),
		"visibilityTimeout":   str.Int642str(queuemeta.VisibilityTimeout),
		"maxMsgSize":          str.Int642str(queuemeta.MaxMsgSize),
		"msgRetentionSeconds": str.Int642str(queuemeta.MsgRetentionSeconds),
		"rewindSeconds":       str.Int642str(queuemeta.RewindSeconds),
	}
	if queuemeta.MaxMsgHeapNum > 0 {
		params["maxMsgHeapNum"] = str.Int642str(queuemeta.MaxMsgHeapNum)
	}
	_, err = q.CmqClient.CreateQueue(params)
	return
}

/*
@type queueName: queueName string
@return err
CreateByName -用名字创建队列
*/
func (q *CmqQueue) CreateByName(queueName string) (err error) {
	return q.Create(q.SetDefaultQueueMeta(queueName))
}

/*
@type backTrackingTime backTrackingTime int64 该时间戳以后的消息
@return err
RewindQueue -回溯队列
*/
func (q *CmqQueue) RewindQueue(backTrackingTime int64) (err error) {
	params := map[string]string{
		"queueName":        q.QueueName,
		"startConsumeTime": str.Int642str(backTrackingTime),
	}
	_, err = q.CmqClient.RewindQueue(params)
	return
}

/*
@return err
Delete -删除队列
*/
func (q *CmqQueue) Delete() (err error) {
	params := map[string]string{
		"queueName": q.QueueName,
	}
	_, err = q.CmqClient.DeleteQueue(params)
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

/*
@type queueName: queueName string
@return QueueMeta QueueMeta struct
*SetAttributes -设置默认的属性
*/
func (q *CmqQueue) SetDefaultQueueMeta(queuename string) *QueueMeta {
	return &QueueMeta{
		Queuename:           queuename,
		MaxMsgHeapNum:       10000000,
		PollingWaitSeconds:  30,
		VisibilityTimeout:   30,
		MaxMsgSize:          1024 * 1024,
		MsgRetentionSeconds: 1296000, //保留15天
		RewindSeconds:       1296000,
	}
}

/*
@type queuemeta: QueueMeta struct
@return err
*SetAttributes -设置属性
*/
func (q *CmqQueue) SetAttributes(queuemeta *QueueMeta) (err error) {
	params := map[string]string{
		"queueName":           q.QueueName,
		"pollingWaitSeconds":  str.Int642str(queuemeta.PollingWaitSeconds),
		"visibilityTimeout":   str.Int642str(queuemeta.VisibilityTimeout),
		"maxMsgSize":          str.Int642str(queuemeta.MaxMsgSize),
		"msgRetentionSeconds": str.Int642str(queuemeta.MsgRetentionSeconds),
		"rewindSeconds":       str.Int642str(queuemeta.RewindSeconds),
	}
	if queuemeta.MaxMsgHeapNum > 0 {
		params["maxMsgHeapNum"] = str.Int642str(queuemeta.MaxMsgHeapNum)
	}
	_, err = q.CmqClient.SetQueueAttributes(params)
	return
}

/*
return QueueMeta QueueMeta struct
return err
GetAttributes -获取属性
*/
func (q *CmqQueue) GetAttributes() (queuemeta *QueueMeta, err error) {
	params := map[string]string{
		"queueName": q.QueueName,
	}
	var res *GetQueueAttributesRes
	res, err = q.CmqClient.GetQueueAttributes(params)
	queuemeta = &QueueMeta{
		Queuename:           q.QueueName,
		PollingWaitSeconds:  res.PollingWaitSeconds,
		VisibilityTimeout:   res.VisibilityTimeout,
		MaxMsgSize:          res.MaxMsgSize,
		MsgRetentionSeconds: res.MsgRetentionSeconds,
		RewindSeconds:       res.RewindSeconds,
	}
	return
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
@param queuename string
@param message struct
@param delaytime int64
@return
SendMessage -发送消息
*/
func (q *CmqQueue) SendMessage(message *Message, delaytime int64) (msgid string, err error) {
	params := map[string]string{
		"queueName":    q.QueueName,
		"msgBody":      message.MsgBody,
		"delaySeconds": str.Int642str(delaytime),
	}
	//TODOS消息加密
	msgid, err = q.CmqClient.SendMessage(params)
	return
}

/*
@type messages []*Struct
@params messages 批量的消息列
@type delaytime int64
@params delaytime 发送消息后,需要延时多久用户才可见
@rtype msgids []string
@params msgids 消息的唯一标识列
BatchSendMessage -批量发送消息
*/
func (q *CmqQueue) BatchSendMessage(messages []*Message, delaytime int64) (msgids []string, err error) {
	params := map[string]string{
		"queueName":    q.QueueName,
		"delaySeconds": str.Int642str(delaytime),
	}
	//TODOS消息加密
	for k, message := range messages {
		var key = fmt.Sprintf("msgBody.%v", k)
		params[key] = message.MsgBody
	}
	var batchSendMessageres *BatchSendMessageRes
	batchSendMessageres, err = q.CmqClient.BatchSendMessage(params)

	for _, msglist := range batchSendMessageres.MsgList {
		msgids = append(msgids, msglist.MsgID)
	}
	return
}

/*
@type pollingwaitseconds: int
@param pollingwaitseconds: 本次请求的长轮询时间，单位: 秒

@rtype Message object
@return Message object 中包含基本属性、 临时句柄
ReceiveMessage -消费消息
*/
func (q *CmqQueue) ReceiveMessage(pollingwaitseconds int64) (msg *Message, err error) {
	params := map[string]string{
		"queueName": q.QueueName,
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

/*
@type numOfMsg int64
@params numOfMsg 本次消费的消息数量
@type pollingWaitSeconds int64
@params pollingWaitSeconds 本次请求的长轮询等待时间
BatchReceiveMessage - 批量消费消息
*/
func (q *CmqQueue) BatchReceiveMessage(numOfMsg int64, pollingWaitSeconds int64) (messages []*Message, err error) {
	params := map[string]string{
		"queueName": q.QueueName,
		"numOfMsg":  str.Int642str(numOfMsg),
	}

	if pollingWaitSeconds != 0 {
		params["UserpollingWaitSeconds"] = str.Int642str(pollingWaitSeconds)
		params["pollingWaitSeconds"] = str.Int642str(pollingWaitSeconds)
	} else {
		params["UserpollingWaitSeconds"] = str.Int2str(30)
	}
	var batchreceivemessageres *BatchReceiveMessageRes
	batchreceivemessageres, err = q.CmqClient.BatchReceiveMessage(params)

	for _, msginfolist := range batchreceivemessageres.MsgInfoList {
		var message = &Message{
			MsgBody:          msginfolist.MsgBody,
			MsgID:            msginfolist.MsgID,
			ReceiptHandle:    msginfolist.ReceiptHandle,
			EnqueueTime:      str.Str2int(msginfolist.EnqueueTime),
			FirstDequeueTime: str.Str2int(msginfolist.FirstDequeueTime),
			NextVisibleTime:  str.Str2int(msginfolist.NextVisibleTime),
			DequeueCount:     str.Str2int(msginfolist.DequeueCount),
		}
		messages = append(messages, message)
	}
	return
}

/*
@type receipthandle string
@params receipthandle 上次消费返回唯一的消息句柄,用于删除消息
@rtype error
@return err
DeleteMessage - 删除消息
*/
func (q *CmqQueue) DeleteMessage(receipthandle string) (err error) {
	params := map[string]string{
		"queueName":     q.QueueName,
		"receiptHandle": receipthandle,
	}
	_, err = q.CmqClient.DeleteMessage(params)
	return
}

/*
@type receipthandlelist []string
@params receipthandlelist 上次消费消息时返回的消息句柄，从0或者从1开始
@rtype error
@return err
BatchDeleteMessage - 批量删除消息
*/
func (q *CmqQueue) BatchDeleteMessage(receipthandlelist []string) (err error) {
	params := map[string]string{
		"queueName": q.QueueName,
	}

	for k, receipthandle := range receipthandlelist {
		var key = fmt.Sprintf("receiptHandle.%v", k)
		params[key] = receipthandle
	}

	_, err = q.CmqClient.BatchDeleteMessage(params)
	return
}
