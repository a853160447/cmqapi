package cmqapi

import (
	"fmt"

	"github.com/friendlyhank/foundation/str"
)

//CmqTopic -
type CmqTopic struct {
	TopicName string
	CmqClient *CmqClient
	Encoding  bool //64位编码
}

//NewCmqTopic -
func NewCmqTopic(topicname string, cmqclient *CmqClient, encoding bool) *CmqTopic {
	return &CmqTopic{TopicName: topicname, CmqClient: cmqclient, Encoding: encoding}
}

/*
type topicmeta Struct
params topicmeta 主题属性设置
@rtype err
@return error
*/
func (t *CmqTopic) Create(topicmeta *TopicMeta) (err error) {
	params := map[string]string{
		"topicName":  t.TopicName,
		"filterType": fmt.Sprintf("%v", topicmeta.FilterType),
	}
	if topicmeta.MaxMsgSize > 0 {
		params["maxMsgSize"] = str.Int642str(topicmeta.MaxMsgSize)
	}
	_, err = t.CmqClient.CreateTopic(params)
	return
}

/*
type maxMsgSize int64
params maxMsgSize 消息最大长度
type filterType 主题的匹配策略
@rtype err
@return error
CreateBySet -根据详情创建主题
*/
func (t *CmqTopic) CreateBySet(maxMsgSize int64, filterType int32) (err error) {
	return t.Create(t.CreateDefaultTopicMeta(maxMsgSize, filterType))
}

//TopicMeta -
type TopicMeta struct {
	MaxMsgSize          int64 //消息最大长度。取值范围1024-65536 Byte（即1-64K），默认值 65536
	MsgRetentionSeconds int64 //消息在主题中最长存活时间,单位为秒，固定为一天(86400 秒)
	CreateTime          int64 //主题创建时间
	LastModifyTime      int64 //最后一次修改主题属性的时间
	FilterType          int32 //用户创建订阅选择的过滤策略。0表示使用filtertype 标签过滤；1表示使用bindingKey过滤
}

//CreateDefaultTopicMeta -
/*
type maxMsgSize int64
params  maxMsgSize 消息最大长度
type filterType int32
*/
func (t *CmqTopic) CreateDefaultTopicMeta(maxMsgSize int64, filterType int32) *TopicMeta {
	return &TopicMeta{
		MaxMsgSize: maxMsgSize,
		FilterType: filterType,
	}
}

/*
type maxMsgSize int64
params maxMsgSize 消息最大长度
rtype err error
return err 错误
*/
func (t *CmqTopic) SetTopicAttributes(maxMsgSize int64) (err error) {
	params := map[string]string{
		"topicName":  t.TopicName,
		"maxMsgSize": str.Int642str(maxMsgSize),
	}
	_, err = t.CmqClient.SetTopicAttributes(params)
	return
}

/*
rtype topicmeta struct
return topicmeta 主题属性
rtype err error
return err 错误
GetTopicAttributes -获取主题属性
*/
func (t *CmqTopic) GetTopicAttributes() (topicmeta *TopicMeta, err error) {
	params := map[string]string{
		"topicName": t.TopicName,
	}
	var getTopicAttributesRes *GetTopicAttributesRes
	getTopicAttributesRes, err = t.CmqClient.GetTopicAttributes(params)

	topicmeta = &TopicMeta{
		MaxMsgSize:          getTopicAttributesRes.MaxMsgSize,
		MsgRetentionSeconds: getTopicAttributesRes.MsgRetentionSeconds,
		CreateTime:          getTopicAttributesRes.CreateTime,
		LastModifyTime:      getTopicAttributesRes.LastModifyTime,
		FilterType:          getTopicAttributesRes.FilterType,
	}
	return
}
