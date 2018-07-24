package cmqapi

import (
	"sync"

	"git.biezao.com/ant/xmiss/foundation/vars"
	"github.com/friendlyhank/foundation/str"
)

var (
	defAccount = &Account{
		AppID:     "1253835554",
		SecretID:  "AKIDnibJrEbqTwKXZWN3c1IDFIB80hIohKK9",
		SecretKey: "0Rn7PYYP5au43qW0MYZO2OdkiV7cxFMI",
	}
)

var (
	once           sync.Once
	defaultAccount *Account
)

//Account -
type Account struct {
	AppID     string
	SecretID  string
	SecretKey string
	CmqClient *CmqClient
}

//NewAccount -
func (a *Account) NewAccount(secretID, secretKey, region string, isinner bool) *Account {
	return &Account{
		SecretID:  secretID,
		SecretKey: secretKey,
		CmqClient: NewCmqClient(secretID, secretKey, region, isinner),
	}
}

//GetDefaultAccount -获取默认的账户信息
func GetDefaultAccount() *Account {
	once.Do(func() {
		defaultAccount = &Account{
			SecretID:  defAccount.SecretID,
			SecretKey: defaultAccount.SecretKey,
			CmqClient: NewCmqClient(vars.Cmq.SecretID, vars.Cmq.SecretKey, vars.Cmq.Region, vars.Cmq.Isinner),
		}
	})
	return defaultAccount
}

/*
GetClient -获取客户端信息
rtype:CMQClient object
return: 返回使用CMQClient object
*/
func (a *Account) GetClient() *CmqClient {
	return a.CmqClient
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
GetDefaultAccountQueue -获取默认Account的一个queue对象
*/
func GetDefaultAccountQueue(queuename string) *CmqQueue {
	return GetDefaultAccount().GetQueue(queuename)
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

/**
*CreateQueueIfNotExist -队列不存在则默认创建一个
@type queueName string
@rtype CmqQueue struct
@return：返回该Account的Queue对象
*
*/
func (a *Account) CreateQueueIfNotExist(queueName string) (*CmqQueue, error) {

	q := a.GetQueue(queueName)

	//检测是否已有队列没有则创建队列
	searchqueue, err := a.ListQueue(q.QueueName, 1, 0)
	if nil != err {
		return nil, err
	}

	//没有队列则创建一个队列
	if searchqueue != nil && searchqueue.TotalCount == 0 {
		//创建队列
		// err := q.CreateByName(queueName)
		if nil != err {
			return nil, err
		}
	}
	return q, nil
}
