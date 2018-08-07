package cmqapi

import (
	"fmt"
	"net/url"
	"time"

	"git.biezao.com/ant/xmiss/foundation/profile"
	xhttp "github.com/friendlyhank/foundation/http"
	"github.com/friendlyhank/foundation/str"
)

//CmqClient -
type CmqClient struct {
	IsInner   bool   //是否内网
	Host      string //当前的Host
	Region    string //地区
	InnerAddr string //内网地址
	OutAddr   string //外网地址
	Version   string //版本号
	HTTP      string
	Method    string
	secretID  string
	secretKey string
}

//result -
type result struct {
	Code      int    `json:"code"`
	Message   string `json:"message"`
	RequestID string `json:"requestId"`
}

//CreatequeueRes -创建队列
type CreatequeueRes struct {
	result
	QueueID string `json:"queueId"`
}

//QueueList -
type QueueList struct {
	QueueID   string `json:"queueId"`
	QueueName string `json:"queueName"`
}

//ListQueueRes -队列列表
type ListQueueRes struct {
	result
	TotalCount int64 `json:"totalCount"`
	//record_hank
	QueueList []QueueList `json:"queueList"`
}

//DeleteQueueRes -删除队列
type DeleteQueueRes struct {
	result
}

//RewindQueueRes -回溯队列
type RewindQueueRes struct {
	result
}

//SetQueueAttributesRes -设置队列属性
type SetQueueAttributesRes struct {
	result
	MaxMsgHeapNum       int64 `json:"maxMsgHeapNum"`
	PollingWaitSeconds  int64 `json:"pollingWaitSeconds"`
	VisibilityTimeout   int64 `json:"visibilityTimeout"`
	MaxMsgSize          int64 `json:"maxMsgSize"`
	MsgRetentionSeconds int64 `json:"msgRetentionSeconds"`
	RewindSeconds       int64 `json:"rewindSeconds"`
}

//GetQueueAttributesRes - 获取队列属性
type GetQueueAttributesRes struct {
	result
	MaxMsgHeapNum       int64 `json:"maxMsgHeapNum"`
	PollingWaitSeconds  int64 `json:"pollingWaitSeconds"`
	VisibilityTimeout   int64 `json:"visibilityTimeout"`
	MaxMsgSize          int64 `json:"maxMsgSize"`
	MsgRetentionSeconds int64 `json:"msgRetentionSeconds"`
	CreateTime          int64 `json:"createTime"`
	LastModifyTime      int64 `json:"lastModifyTime"`
	ActiveMsgNum        int64 `json:"activeMsgNum"`
	InactiveMsgNum      int64 `json:"inactiveMsgNum"`
	RewindSeconds       int64 `json:"rewindSeconds"`
	RewindmsgNum        int64 `json:"rewindmsgNum"`
	MinMsgTime          int64 `json:"minMsgTime"`
}

//SendMessageRes -发送消息
type SendMessageRes struct {
	result
	MsgID string `json:"msgId"`
}

//MsgList -
type MsgList struct {
	MsgID string `json:"msgId"`
}

//BatchSendMessageRes -批量发送消息
type BatchSendMessageRes struct {
	result
	MsgList []MsgList `json:"msgList"`
}

//ReceiveMessageRes -消费消息
type ReceiveMessageRes struct {
	result
	MsgBody          string `json:"msgBody"`
	MsgID            string `json:"msgId"`
	ReceiptHandle    string `json:"receiptHandle"`
	EnqueueTime      int64  `json:"enqueueTime"`
	FirstDequeueTime int64  `json:"firstDequeueTime"`
	NextVisibleTime  int64  `json:"nextVisibleTime"`
	DequeueCount     int64  `json:"dequeueCount"`
}

//MsgInfoList -
type MsgInfoList struct {
	MsgBody          string `json:"msgBody"`
	MsgID            string `json:"msgId"`
	ReceiptHandle    string `json:"receiptHandle"`
	EnqueueTime      string `json:"enqueueTime"`
	FirstDequeueTime string `json:"firstDequeueTime"`
	NextVisibleTime  string `json:"nextVisibleTime"`
	DequeueCount     string `json:"dequeueCount"`
}

//BatchReceiveMessageRes -批量消费消息
type BatchReceiveMessageRes struct {
	result
	MsgInfoList []MsgInfoList `json:"msgInfoList"`
}

//DeleteMessageRes -删除消息
type DeleteMessageRes struct {
	result
}

//ErrorList -
type ErrorList struct {
	Code          string `json:"code"`
	Message       string `json:"message"`
	ReceiptHandle string `json:"receiptHandle"`
}

//BatchDeleteMessageRes -批量删除消息
type BatchDeleteMessageRes struct {
	result
	ErrorList []ErrorList `json:"errorList"`
}

//CreateTopicRes -创建主题
type CreateTopicRes struct {
	result
	TopicID string `json:"topicId"`
}

//TopicList -
type TopicList struct {
	TopicID   string `json:"topicId"`
	TopicName string `json:"topicName"`
}

//ListTopicRes - 获取主题列表
type ListTopicRes struct {
	result
	TopicList []TopicList
}

//DeleteTopicRes -删除主题
type DeleteTopicRes struct {
	result
}

//SetTopicAttributesRes - 设置主题属性
type SetTopicAttributesRes struct {
	result
}

//GetTopicAttributesRes -获取主题属性
type GetTopicAttributesRes struct {
	result
	MsgCount            int64 `json:"msgCount"`
	MaxMsgSize          int64 `json:"maxMsgSize"`
	MsgRetentionSeconds int64 `json:"msgRetentionSeconds"`
	CreateTime          int64 `json:"createTime"`
	LastModifyTime      int64 `json:"lastModifyTime"`
	FilterType          int64 `json:"filterType"`
}

//PublishMessageRes -用于发布一条消息到指定的主题
type PublishMessageRes struct {
	result
	MsgID string `json:"msgId"`
}

//BatchPublishMessageRes - 用于发布批量(目前最多16条)消息到指定的主题
type BatchPublishMessageRes struct {
	result
	MsgList []MsgList `json:"msgList"`
}

//CreateSubscribeRes -创建订阅
type CreateSubscribeRes struct {
	result
}

//SubScriptionList -订阅列
type SubScriptionList struct {
	SubscriptionID   string `json:"subscriptionId"`
	SubscriptionName string `json:"subscriptionName"`
	Protocol         string `json:"protocol"`
	Endpoint         string `json:"endpoint"`
}

//ListSubscribeRes -获取订阅列表
type ListSubscribeRes struct {
	result
	TotalCount       int64              `json:"totalCount"`
	SubScriptionList []SubScriptionList `json:"subscriptionList"`
}

//SetSubScriptionAttributesRes -修改订阅属性
type SetSubScriptionAttributesRes struct {
	result
}

//GetSubScriptionAttributesRes - 获取订阅属性
type GetSubScriptionAttributesRes struct {
	result
	SubScriptionList []SubScriptionList
}

//ClearFilterTagsRes -
type ClearFilterTagsRes struct {
	result
}

//DeleteSubscribeRes -删除订阅
type DeleteSubscribeRes struct {
	result
}

//NewCmqClient -
func NewCmqClient(secretID, secretKey, region string, IsInner bool) (cmqclient *CmqClient) {
	cmqclient = &CmqClient{
		secretID:  secretID,
		secretKey: secretKey,
		Region:    region,
		Version:   "SDKGO1.0",
		HTTP:      "",
		Method:    "GET",
		IsInner:   IsInner,
	}
	cmqclient.InnerAddr = "http://cmq-queue-" + region + ".api.tencentyun.com/v2/index.php"
	cmqclient.OutAddr = "https://cmq-queue-" + region + ".api.qcloud.com/v2/index.php"
	cmqclient.Host = cmqclient.getHost(IsInner)
	return cmqclient
}

func (c *CmqClient) getHost(isInner bool) string {
	return str.CaseString(isInner, c.InnerAddr, c.OutAddr)
}

//BuildReqInter -初始化参数
func (c *CmqClient) BuildReqInter(action string, params map[string]string) (cparams map[string]string, err error) {
	//公共参数
	commonparams := make(map[string]string, 0)
	commonparams["Action"] = action
	commonparams["Region"] = c.Region
	commonparams["RequestClient"] = c.Version
	commonparams["Timestamp"] = fmt.Sprintf("%v", time.Now().Unix())
	commonparams["Nonce"] = fmt.Sprintf("%v", time.Now().Unix())
	commonparams["SecretId"] = defAccount.SecretID
	commonparams["SignatureMethod"] = "HmacSHA1"

	//加入公共参数
	for i, k := range commonparams {
		params[i] = k
	}

	s := &Sign{}
	plaintext, err := s.MakeSignPlainText(params)

	params["Signature"], err = s.Sign(plaintext, defAccount.SecretKey)
	cparams = params

	return
}

//===============================================queue operation===============================================

/**
{
"queueName":"QUEUENAME",
"maxMsgHeapNum":"MAXMSGHEAPNUM"
"pollingWaitSeconds":"POLLINGWAITSECONDS",
"visibilityTimeout":"VISIBILITYTIMEOUT"
"maxMsgSize":"MAXMSGSIZE",
"msgRetentionSeconds":"MSGRETENTIONSECONDS"
"rewindSeconds":"REWINDSECONDS",
"deadLetterPolicy":"DEADLETTERPOLICY",
}
{
"code":"CODE",
"message":"MESSAGE"
"requestId":"REQUESTID",
"queueId":"QUEUEID"
}
**/
//CreateQueue -创建队列
func (c *CmqClient) CreateQueue(params map[string]string) (createqueueres *CreatequeueRes, err error) {
	defer profile.TimeTrack(time.Now(), "[Wx-API] Createqueue")

	//拼接参数
	cparams, _ := c.BuildReqInter("CreateQueue", params)
	//转换请求
	values := GetURLValus(cparams)
	createqueueres = &CreatequeueRes{}
	err = getURL(c.Host, values, createqueueres)
	return
}

/**
{
"searchWord":"SEARCHWORD",
"offset":"OFFSET"
"limit":"LIMIT",
}
{
"code":"CODE",
"message":"MESSAGE"
"requestId":"REQUESTID",
"totalCount":"TOTALCOUNT"
"queueList":"QUEUELIST"
}
**/
//ListQueue -队列列表
func (c *CmqClient) ListQueue(params map[string]string) (listqueueres *ListQueueRes, err error) {
	defer profile.TimeTrack(time.Now(), "[Wx-API] ListQueue")

	//拼接参数
	cparams, _ := c.BuildReqInter("ListQueue", params)
	//转换请求
	values := GetURLValus(cparams)
	listqueueres = &ListQueueRes{}
	err = getURL(c.Host, values, listqueueres)
	return
}

/**
{
"queueName":"QUEUENAME",
}
{
"code":"CODE",
"message":"MESSAGE"
"requestId":"REQUESTID",
}
**/
//DeleteQueue -删除队列
func (c *CmqClient) DeleteQueue(params map[string]string) (deletequeueres *DeleteQueueRes, err error) {
	defer profile.TimeTrack(time.Now(), "[Wx-API] DeleteQueue")

	//拼接参数
	cparams, _ := c.BuildReqInter("DeleteQueue", params)
	//转换请求
	values := GetURLValus(cparams)
	deletequeueres = &DeleteQueueRes{}
	err = getURL(c.Host, values, deletequeueres)
	return
}

/**
{
"queueName":"QUEUENAME",
"startConsumeTime":"STARTCONSUMETIME",
}
{
"code":"CODE",
"message":"MESSAGE"
"requestId":"REQUESTID",
}
**/
//RewindQueue -回溯队列
func (c *CmqClient) RewindQueue(params map[string]string) (rewindqueueres *RewindQueueRes, err error) {
	defer profile.TimeTrack(time.Now(), "[Wx-API] RewindQueue")

	//拼接参数
	cparams, _ := c.BuildReqInter("RewindQueue", params)
	//转换请求
	values := GetURLValus(cparams)
	rewindqueueres = &RewindQueueRes{}
	err = getURL(c.Host, values, rewindqueueres)
	return
}

/*
"queueName":"QUEUENAME",
"maxMsgHeapNum":"maxMsgHeapNum",
"pollingWaitSeconds":"pollingWaitSeconds",
"visibilityTimeout":"visibilityTimeout",
"maxMsgSize":"maxMsgSize",
"msgRetentionSeconds":"msgRetentionSeconds",
"rewindSeconds	":"rewindSeconds",
}
{
"code":"CODE",
"message":"MESSAGE"
"requestId":"REQUESTID",
"maxMsgHeapNum":"maxMsgHeapNum",
"pollingWaitSeconds":"pollingWaitSeconds"
"visibilityTimeout":"visibilityTimeout",
"maxMsgSize":"maxMsgSize",
"msgRetentionSeconds":"msgRetentionSeconds"
"rewindSeconds	":"rewindSeconds	",
}
*SetQueueAttributes - 设置属性
*/
func (c *CmqClient) SetQueueAttributes(params map[string]string) (setQueueAttributesRes *SetQueueAttributesRes, err error) {
	defer profile.TimeTrack(time.Now(), "[Wx-API] SetQueueAttributes")

	//拼接参数
	cparams, _ := c.BuildReqInter("SetQueueAttributes", params)
	//转换请求
	values := GetURLValus(cparams)
	setQueueAttributesRes = &SetQueueAttributesRes{}
	err = getURL(c.Host, values, setQueueAttributesRes)
	return
}

/*
"queueName":"QUEUENAME",
}
{
"code":"CODE",
"message":"MESSAGE"
"requestId":"REQUESTID",
"maxMsgHeapNum":"maxMsgHeapNum",
"pollingWaitSeconds":"pollingWaitSeconds"
"visibilityTimeout":"visibilityTimeout",
"maxMsgSize":"maxMsgSize",
"msgRetentionSeconds":"msgRetentionSeconds"
"createTime":"createTime"
"lastModifyTime":"lastModifyTime"
"activeMsgNum":"activeMsgNum"
"inactiveMsgNum":"inactiveMsgNum"
"rewindSeconds":"rewindSeconds"
"rewindmsgNum":"rewindmsgNum"
"minMsgTime	":"minMsgTime"
}
*GetQueueAttributes -获取队列属性
*/
func (c *CmqClient) GetQueueAttributes(params map[string]string) (getQueueAttributesRes *GetQueueAttributesRes, err error) {
	defer profile.TimeTrack(time.Now(), "[Wx-API] GetQueueAttributes")

	//拼接参数
	cparams, _ := c.BuildReqInter("GetQueueAttributes", params)
	//转换请求
	values := GetURLValus(cparams)
	getQueueAttributesRes = &GetQueueAttributesRes{}
	err = getURL(c.Host, values, getQueueAttributesRes)
	return
}

/**
{
"queueName":"QUEUENAME",
"msgBody":"MSGBODY"
"delaySeconds":"DELAYSECONDS",
}
{
"code":"CODE",
"message":"MESSAGE"
"requestId":"REQUESTID",
"msgId":"MSGID"
}
**/
//SendMessage -发送消息
func (c *CmqClient) SendMessage(params map[string]string) (msgid string, err error) {
	defer profile.TimeTrack(time.Now(), "[Wx-API] SendMessage")
	//拼接参数
	cparams, _ := c.BuildReqInter("SendMessage", params)
	//转换请求
	values := GetURLValus(cparams)
	sendmessageres := &SendMessageRes{}
	err = getURL(c.Host, values, sendmessageres)
	if err == nil && sendmessageres != nil {
		msgid = sendmessageres.MsgID
	}
	return
}

/**
{
"queueName":"QUEUENAME",
"msgBody.n":"MSGBODYN"
}
{
"code":"CODE",
"message":"MESSAGE"
"requestId":"REQUESTID",
"msgList":"MSGLIST"
}
*/
//BatchSendMessage -批量发送消息
func (c *CmqClient) BatchSendMessage(params map[string]string) (batchSendMessageres *BatchSendMessageRes, err error) {
	defer profile.TimeTrack(time.Now(), "[Wx-API] BatchSendMessage")
	//拼接参数
	cparams, _ := c.BuildReqInter("BatchSendMessage", params)
	//转换请求
	values := GetURLValus(cparams)
	batchSendMessageres = &BatchSendMessageRes{}
	err = getURL(c.Host, values, batchSendMessageres)
	return
}

/**
{
"queueName":"QUEUENAME",
"pollingWaitSeconds":"POLLINGWAITSECONDS"
}
{
"code":"CODE",
"message":"MESSAGE"
"requestId":"REQUESTID",
"msgBody":"MSGBODY"
"msgId":"MSGID"
"receiptHandle":"RECEIPTHANDLE"
"enqueueTime":"ENQUEUETIME"
"firstDequeueTime":"FIRSTDEQUEUETIME"
"nextVisibleTime":"NEXTVISBLETIME"
"dequeueCount":"DEQUEUECOUNT"
}
**/
//ReceiveMessage -用于消费队列中的一条消息
func (c *CmqClient) ReceiveMessage(params map[string]string) (receivemessageres *ReceiveMessageRes, err error) {
	defer profile.TimeTrack(time.Now(), "[Wx-API] ReceiveMessage")
	//拼接参数
	cparams, _ := c.BuildReqInter("ReceiveMessage", params)
	//转换请求
	values := GetURLValus(cparams)
	receivemessageres = &ReceiveMessageRes{}
	err = getURL(c.Host, values, receivemessageres)
	return
}

/**
{
"queueName":"QUEUENAME",
"numOfMsg":"NUMOFMSG"
"pollingWaitSeconds":"POLLINGWAITSECONDS"
}
{
"code":"CODE",
"message":"MESSAGE"
"requestId":"REQUESTID",
"msgInfoList":"MSGINFOLIST"
}
**/
//BatchReceiveMessage -批量消费消息
func (c *CmqClient) BatchReceiveMessage(params map[string]string) (batchreceivemessageres *BatchReceiveMessageRes, err error) {
	defer profile.TimeTrack(time.Now(), "[Wx-API] BatchReceiveMessage")
	//拼接参数
	cparams, _ := c.BuildReqInter("BatchReceiveMessage", params)
	//转换请求
	values := GetURLValus(cparams)
	batchreceivemessageres = &BatchReceiveMessageRes{}
	err = getURL(c.Host, values, batchreceivemessageres)
	return
}

/**
{
"queueName":"QUEUENAME",
"receiptHandle":"RECEIPTHANDLE"
}
{
"code":"CODE",
"message":"MESSAGE"
"requestId":"REQUESTID",
}
**/
//DeleteMessage -删除消息
func (c *CmqClient) DeleteMessage(params map[string]string) (deletemessageres *DeleteMessageRes, err error) {
	defer profile.TimeTrack(time.Now(), "[Wx-API] DeleteMessage")
	//拼接参数
	cparams, _ := c.BuildReqInter("DeleteMessage", params)
	//转换请求
	values := GetURLValus(cparams)
	deletemessageres = &DeleteMessageRes{}
	err = getURL(c.Host, values, deletemessageres)
	return
}

/**
{
"queueName":"QUEUENAME",
"receiptHandle.n":"RECEIPTHANDLE.N"
}
{
"code":"CODE",
"message":"MESSAGE"
"requestId":"REQUESTID",
"errorList":"ERRORLIST",
}
**/
//BatchDeleteMessage -批量删除消息
func (c *CmqClient) BatchDeleteMessage(params map[string]string) (batchdeletemessageres *BatchDeleteMessageRes, err error) {
	defer profile.TimeTrack(time.Now(), "[Wx-API] BatchDeleteMessage")
	//拼接参数
	cparams, _ := c.BuildReqInter("BatchDeleteMessage", params)
	//转换请求
	values := GetURLValus(cparams)
	batchdeletemessageres = &BatchDeleteMessageRes{}
	err = getURL(c.Host, values, batchdeletemessageres)
	return
}

//===============================================topic operation===============================================

/*
{
"topicName":"TOPICNAME",
"maxMsgSize":"MAXMSGSIZE"
"filterType":"FILTERTYPE",
}

{
"code":"CODE",
"message":"MESSAGE"
"requestId":"REQUESTID",
"topicId":"TOPICID"
}
CreateTopic -创建主题
*/
func (c *CmqClient) CreateTopic(params map[string]string) (createtopicres *CreateTopicRes, err error) {
	defer profile.TimeTrack(time.Now(), "[Wx-API] CreateTopic")
	//拼接参数
	cparams, _ := c.BuildReqInter("CreateTopic", params)
	//转换请求
	values := GetURLValus(cparams)
	createtopicres = &CreateTopicRes{}
	err = getURL(c.Host, values, createtopicres)
	return
}

/*
{
"topicName":"TOPICNAME",
"maxMsgSize":"MAXMSGSIZE"

}
{
"code":"CODE",
"message":"MESSAGE"
"requestId":"REQUESTID",
}
SetTopicAttributes - 设置主题属性
*/
func (c *CmqClient) SetTopicAttributes(params map[string]string) (settopicattributesres *SetTopicAttributesRes, err error) {
	defer profile.TimeTrack(time.Now(), "[Wx-API] SetTopicAttributes")
	//拼接参数
	cparams, _ := c.BuildReqInter("SetTopicAttributes", params)
	//转换请求
	values := GetURLValus(cparams)
	settopicattributesres = &SetTopicAttributesRes{}
	err = getURL(c.Host, values, settopicattributesres)
	return
}

/*
{
"topicName":"TOPICNAME",
}
{
"code":"CODE",
"message":"MESSAGE"
"requestId":"REQUESTID",
"msgCount":"MSGCOUNT",
"maxMsgSize":"MAXMSGSIZE"
"msgRetentionSeconds":"MSGRETENTIONSECONDS",
"createTime":"CREATETIME",
"lastModifyTime":"LASTMODIFYTIME"
"filterType":"FILTERTYPE",
}
GetTopicAttributes - 获取主题属性
*/
func (c *CmqClient) GetTopicAttributes(params map[string]string) (getTopicAttributesRes *GetTopicAttributesRes, err error) {
	defer profile.TimeTrack(time.Now(), "[Wx-API] GetTopicAttributes")
	//拼接参数
	cparams, _ := c.BuildReqInter("GetTopicAttributes", params)
	//转换请求
	values := GetURLValus(cparams)
	getTopicAttributesRes = &GetTopicAttributesRes{}
	err = getURL(c.Host, values, getTopicAttributesRes)
	return
}

/**
{
"topicName":"TOPICNAME",
}
{
"code":"CODE",
"message":"MESSAGE"
"requestId":"REQUESTID",
}
**/
//DeleteTopic -删除主题
func (c *CmqClient) DeleteTopic(params map[string]string) (deletetopicres *DeleteTopicRes, err error) {
	defer profile.TimeTrack(time.Now(), "[Wx-API] DeleteTopic")
	//拼接参数
	cparams, _ := c.BuildReqInter("DeleteTopic", params)
	//转换请求
	values := GetURLValus(cparams)
	deletetopicres = &DeleteTopicRes{}
	err = getURL(c.Host, values, deletetopicres)
	return
}

/**
{
"topicName":"TOPICNAME",
"msgBody":"MSGBODY",
"msgTag.n":"MSGTAGN",
"routingKey":"ROUTINGKEY",
}
{
"code":"CODE",
"message":"MESSAGE"
"requestId":"REQUESTID",
}
**/
//PublishMessage -
func (c *CmqClient) PublishMessage(params map[string]string) (publishmessage *PublishMessageRes, err error) {
	defer profile.TimeTrack(time.Now(), "[Wx-API] PublishMessage")
	//拼接参数
	cparams, _ := c.BuildReqInter("PublishMessage", params)
	//转换请求
	values := GetURLValus(cparams)
	publishmessage = &PublishMessageRes{}
	err = getURL(c.Host, values, publishmessage)
	return
}

/**
{
"topicName":"TOPICNAME",
"msgBody":"MSGBODY",
"msgTag.n":"MSGTAGN",
"routingKey":"ROUTINGKEY",
}
{
"code":"CODE",
"message":"MESSAGE"
"requestId":"REQUESTID",
}
**/
//BatchPublishMessage -批量发送消息
func (c *CmqClient) BatchPublishMessage(params map[string]string) (batchpublishmessageres *BatchPublishMessageRes, err error) {
	defer profile.TimeTrack(time.Now(), "[Wx-API] BatchPublishMessage")
	//拼接参数
	cparams, _ := c.BuildReqInter("BatchPublishMessage", params)
	//转换请求
	values := GetURLValus(cparams)
	batchpublishmessageres = &BatchPublishMessageRes{}
	err = getURL(c.Host, values, batchpublishmessageres)
	return
}

//============================================subscription operation=============================================

/*
{
"topicName":"TOPICNAME",
"subscriptionName":"SUBSCRIPTIONNAME",
"protocol":"PROTOCOL",
"endpoint":"ENDPOINT",
"notifyStrategy":"NOTIFYSTRATEGY",
"notifyContentFormat":"NOTIFYCONTENTFORMAT",
"filterTag.n":"FILTERTAG.N",
"bindingKey.n":"BINDINGKEY.N",
}
{
"code":"CODE",
"message":"MESSAGE"
"requestId":"REQUESTID",
}
**/
//CreateSubscription -在用户某个主题下创建一个新订阅
func (c *CmqClient) CreateSubscription(params map[string]string) (createsubscriberes *CreateSubscribeRes, err error) {
	defer profile.TimeTrack(time.Now(), "[Wx-API] Subscribe")

	//拼接参数
	cparams, _ := c.BuildReqInter("Subscribe", params)
	//转换请求
	values := GetURLValus(cparams)
	createsubscriberes = &CreateSubscribeRes{}
	err = getURL(c.Host, values, createsubscriberes)
	return
}

/*
{
"topicName":"TOPICNAME",
"subscriptionName":"SUBSCRIPTIONNAME",
}
{
"code":"CODE",
"message":"MESSAGE"
"requestId":"REQUESTID",
}
CreateSubscription -清空订阅标签
*/
func (c *CmqClient) ClearFilterTags(params map[string]string) (clearFilterTagsRes *ClearFilterTagsRes, err error) {
	defer profile.TimeTrack(time.Now(), "[Wx-API] ClearSubscriptionFilterTags")

	//拼接参数
	cparams, _ := c.BuildReqInter("ClearSubscriptionFilterTags", params)
	//转换请求
	values := GetURLValus(cparams)
	clearFilterTagsRes = &ClearFilterTagsRes{}
	err = getURL(c.Host, values, clearFilterTagsRes)
	return
}

/**
{
"topicName":"TOPICNAME",
"searchWord":"SUBSCRIPTIONNAME",
"offset":"PROTOCOL",
"limit":"ENDPOINT",
}
{
"code":"CODE",
"message":"MESSAGE"
"requestId":"REQUESTID",
"totalCount":"TOTALCOUNT",
"subscriptionList":"SUBSCRIPTIONLIST",
}
**/
//ListSubscription -获取订阅列表
func (c *CmqClient) ListSubscription(params map[string]string) (listsubscriberes *ListSubscribeRes, err error) {
	defer profile.TimeTrack(time.Now(), "[Wx-API] ListSubscriptionByTopic")

	//拼接参数
	cparams, _ := c.BuildReqInter("ListSubscriptionByTopic", params)
	//转换请求
	values := GetURLValus(cparams)
	listsubscriberes = &ListSubscribeRes{}
	err = getURL(c.Host, values, listsubscriberes)
	return
}

/*
{
"topicName":"TOPICNAME",
"searchWord":"SUBSCRIPTIONNAME",
"offset":"PROTOCOL",
"limit":"ENDPOINT",
}
{
"code":"CODE",
"message":"MESSAGE"
"requestId":"REQUESTID",
"totalCount":"TOTALCOUNT",
"subscriptionList":"SUBSCRIPTIONLIST",
}
*/
//SetSubScriptionAttributes -修改订阅属性
func (c *CmqClient) SetSubScriptionAttributes(params map[string]string) (setSubScriptionAttributesRes *SetSubScriptionAttributesRes, err error) {
	defer profile.TimeTrack(time.Now(), "[Wx-API] SetSubscriptionAttributes")

	//拼接参数
	cparams, _ := c.BuildReqInter("SetSubscriptionAttributes", params)
	//转换请求
	values := GetURLValus(cparams)
	setSubScriptionAttributesRes = &SetSubScriptionAttributesRes{}
	err = getURL(c.Host, values, setSubScriptionAttributesRes)
	return
}

/*
{
"topicName":"TOPICNAME",
"searchWord":"SUBSCRIPTIONNAME",
"offset":"PROTOCOL",
"limit":"ENDPOINT",
}
{
"code":"CODE",
"message":"MESSAGE"
"requestId":"REQUESTID",
"totalCount":"TOTALCOUNT",
"subscriptionList":"SUBSCRIPTIONLIST",
}
*/
//GetSubScriptionAttributes -获取订阅属性
func (c *CmqClient) GetSubScriptionAttributes(params map[string]string) (getSubScriptionAttributesRes *GetSubScriptionAttributesRes, err error) {
	defer profile.TimeTrack(time.Now(), "[Wx-API] GetSubscriptionAttributes")

	//拼接参数
	cparams, _ := c.BuildReqInter("GetSubscriptionAttributes", params)
	//转换请求
	values := GetURLValus(cparams)
	getSubScriptionAttributesRes = &GetSubScriptionAttributesRes{}
	err = getURL(c.Host, values, getSubScriptionAttributesRes)
	return
}

//DeleteSubscription -删除订阅
/**
{
"topicName":"TOPICNAME",
"searchWord":"SUBSCRIPTIONNAME",
"offset":"PROTOCOL",
"limit":"ENDPOINT",
}
{
"code":"CODE",
"message":"MESSAGE"
"requestId":"REQUESTID",
"totalCount":"TOTALCOUNT",
"subscriptionList":"SUBSCRIPTIONLIST",
}
**/
func (c *CmqClient) DeleteSubscription(params map[string]string) (deletesubscriberes *DeleteSubscribeRes, err error) {
	defer profile.TimeTrack(time.Now(), "[Wx-API] Unsubscribe")

	//拼接参数
	cparams, _ := c.BuildReqInter("Unsubscribe", params)
	//转换请求
	values := GetURLValus(cparams)
	deletesubscriberes = &DeleteSubscribeRes{}
	err = getURL(c.Host, values, deletesubscriberes)
	return
}

//GetURLValus -设置参数转换
func GetURLValus(params map[string]string) (values url.Values) {
	values = url.Values{}
	for i, v := range params {
		if v == "" {
			continue
		}
		values.Add(i, params[i])
	}

	return
}

//get 基础的网络访问
func getURL(url string, values xhttp.URLEncoder, i interface{}) (err error) {
	err = xhttp.GetJSON(url, values, i)
	return
}

//post 基础的网络访问
func postURL(url string, values xhttp.URLEncoder, req interface{}, res interface{}) (err error) {
	err = xhttp.PostJSON(url, values, req, res)
	return
}
