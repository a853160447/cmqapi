package cmqapi

import "git.biezao.com/ant/xmiss/foundation/util/str"

var (
	defAccount = &Account{
		AppID:     "1253835554",
		SecretID:  "AKIDnibJrEbqTwKXZWN3c1IDFIB80hIohKK9",
		SecretKey: "0Rn7PYYP5au43qW0MYZO2OdkiV7cxFMI",
	}
)

//Account -
type Account struct {
	AppID     string
	SecretID  string
	SecretKey string
	CmqClient *CmqClient
}

//NewAccount -
func (a *Account) NewAccount(region string, isinner bool) *Account {
	defAccount.CmqClient = new(CmqClient).NewClient(region, "SDKGO1.0", "", "GET", isinner)
	return defAccount
}

/*
GetClient -获取客户端信息
rtype:CMQClient object
return: 返回使用CMQClient object
*/
func GetClient() *CmqClient {
	return defAccount.CmqClient
}

/*
GetQueue -获取Account的一个queue对象
@type queuename:string
@param queuename:队列名
@param Encoding默认false,不用64位的编码
@rtype:Queue object
@return:返回Account的一个Queue对象
*/
func (a *Account) GetQueue(queuename string) *CmqQueue {
	return new(CmqQueue).NewCmqQueue(queuename, a.CmqClient, true)
}

/*
ListQueue -
@type searchword string
@param searchword:队列名的前缀

@type limit int64
@param limit:ListQueue最多返回的队列数

@type offset int64
@param offset:ListQueue 分页的起始位置


*/
func (a *Account) ListQueue(searchword string, limit, offset int64) (listqueueres *ListQueueRes, err error) {
	params := make(map[string]string)
	if "" != searchword {
		params["searchWord"] = searchword
	}

	if 0 != limit {
		params["limit"] = str.Int642str(limit)
	}

	if 0 != offset {
		params["offset"] = str.Int642str(offset)
	}

	listqueueres, err = defAccount.CmqClient.ListQueue(params)
	return
}

/**
GetTopic -获取Account的一个topic对象
@type topicname string
@rtype CmqTopic struct
@return：返回该Account的一个Topic对象
*
*/
func (a *Account) GetTopic(topicname string) *CmqTopic {
	return &CmqTopic{TopicName: topicname}
}
