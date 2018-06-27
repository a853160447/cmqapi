package cmqapi

import (
	"crypto/hmac"
	"crypto/sha1"
	"crypto/sha256"
	"encoding/base64"
	"fmt"
	"sort"

	"github.com/astaxie/beego/logs"
)

//Sign -
type Sign struct {
}

/**
*sign
*生成签名
@param string srcstr 拼接签名源文字符串
@param string secretkey secretkey
@param string method 方法
@return string retstr加密串
**/
func (s *Sign) Sign(srcstr, secretkey string) (retstr string, err error) {
	var signmethod = "HmacSHA1"
	switch signmethod {
	case "HmacSHA1":
		retstr = base64.StdEncoding.EncodeToString([]byte(HmacSha1(srcstr, secretkey)))
	case "HmacSHA256":
		retstr = base64.StdEncoding.EncodeToString([]byte(HMacSHA256(srcstr, secretkey)))
	}
	return
}

/**
*MakeSignPlainText
*生成拼接签名源字符串
@param requestparams map[string]interface{}
@return plaintext string
**/
func (s *Sign) MakeSignPlainText(requestparams map[string]string) (plaintext string, err error) {
	var requestmethod = "GET"
	var requesthost = "cmq-queue-gz.api.qcloud.com"
	var requestpath = "/v2/index.php"

	url := requesthost + requestpath

	//拼接参数
	paramstr, _ := s.Buildparamstr(requestparams)
	plaintext = requestmethod + url + paramstr

	logs.Info("|参数拼接|sgin|%v", plaintext)

	return
}

/**
*buildparamstr
*拼接参数
@param map[string]interface{} 请求参数
@return paramstr string	返回拼接参数
**/
func (s *Sign) Buildparamstr(requestparams map[string]string) (paramstr string, err error) {
	//请求数据按A~Z排序
	keys := make([]string, 0)
	for i, v := range requestparams {
		//空字符串
		if v == "" {
			continue
		}
		// if s, ok := v.(string); ok && s == "" {
		// 	continue
		// }
		// if requestparams[i] == nil {
		// 	continue
		// }
		keys = append(keys, i)
	}
	sort.Strings(keys)
	for i, key := range keys {
		if key == "Signature" {
			continue
		}
		if i == 0 {
			paramstr = "?"
		} else {
			paramstr = paramstr + "&"
		}

		paramstr = paramstr + key + "=" + fmt.Sprintf("%v", requestparams[key])
	}

	return
}

//HmacSha1 -
func HmacSha1(data, key string) string {
	mac := hmac.New(sha1.New, []byte(key))
	mac.Write([]byte(data))
	return string(mac.Sum(nil))
}

//HMacSHA256 -
func HMacSHA256(data, key string) string {
	mac := hmac.New(sha256.New, []byte(key))
	mac.Write([]byte(data))
	return string(mac.Sum(nil))
}
