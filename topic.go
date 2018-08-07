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

//TopicMeta -
type TopicMeta struct {
	MaxMsgSize          int64 //消息最大长度。取值范围1024-65536 Byte（即1-64K），默认值 65536
	MsgRetentionSeconds int64 //消息在主题中最长存活时间,单位为秒，固定为一天(86400 秒)
	CreateTime          int64 //主题创建时间
	LastModifyTime      int64 //最后一次修改主题属性的时间
	FilterType          int32 //用户创建订阅选择的过滤策略。0表示使用filtertype 标签过滤；1表示使用bindingKey过滤
}
