package lfm

import (
	"fmt"
	"log"
	"recommend/server"

	"github.com/garyburd/redigo/redis"
	"github.com/tidwall/gjson"
)

type config struct {
	analysisDays                int64   //要分析数据的天数
	cacheSkuNum                 int64   //存入缓存的推荐sku个数
	cacheExpire                 int64   //存入缓存的推荐sku，过期时间
	processNum                  int64   //多线程处理时，开启的线程数
	lfmProcessNum               int64   //LFM计算时，开启的线程数
	sampleVisitorMaxNum         int64   //分析的最大用户数
	sampleSkuMaxNum             int64   //分析的最大SKU数
	positiveSampleMinNum        int64   //正样本最少数量
	offLineDataExpire           int64   //离线数据过期时间
	classCount                  int64   //隐类数量
	iterCount                   int64   //迭代次数
	iterStep                    float64 //迭代步进
	alpha                       float64 //步长
	lamda                       float64 //正则化参数
	multipleOfMaxWeight4Predict float64 //用户停止迭代的Alpha的倍数，强制停止迭代的阈值
	weightOfNegativeItem        int64   //负集合的初始权重（<=0 时直接取设置的值，>0时取0~ 正集合最大权重之间的随机值并转负值）
	actionWeight                string  //用户行为权重
}

func (self *config) getFromCache() {
	redisOnsql := server.GetRedis("nosql")
	defer redisOnsql.Close()
	//从redis获取lfm初始化参数
	nsV, nsErr := redis.String(redisOnsql.Do("HGET", "MYPRJ_HASH_dict_config_all", "recommend_init_params_of_lfm"))
	if nsErr != nil {
		log.Fatalln("[ERROR]", nsErr)
	}
	self.analysisDays = gjson.Get(nsV, "analysisDays").Int()
	self.cacheSkuNum = gjson.Get(nsV, "cacheSkuNum").Int()
	self.cacheExpire = gjson.Get(nsV, "cacheExpire").Int()
	self.processNum = gjson.Get(nsV, "processNum").Int()
	self.lfmProcessNum = gjson.Get(nsV, "lfmProcessNum").Int()
	self.sampleVisitorMaxNum = gjson.Get(nsV, "sampleVisitorMaxNum").Int()
	self.sampleSkuMaxNum = gjson.Get(nsV, "sampleSkuMaxNum").Int()
	self.positiveSampleMinNum = gjson.Get(nsV, "positiveSampleMinNum").Int()
	self.offLineDataExpire = gjson.Get(nsV, "offLineDataExpire").Int()
	self.classCount = gjson.Get(nsV, "classCount").Int()
	self.iterCount = gjson.Get(nsV, "iterCount").Int()
	self.iterStep = gjson.Get(nsV, "iterStep").Float()
	self.alpha = gjson.Get(nsV, "alpha").Float()
	self.lamda = gjson.Get(nsV, "lamda").Float()
	self.multipleOfMaxWeight4Predict = gjson.Get(nsV, "multipleOfMaxWeight4Predict").Float()
	self.weightOfNegativeItem = gjson.Get(nsV, "weightOfNegativeItem").Int()
	self.actionWeight = gjson.Get(nsV, "actionWeight").String()
}

const cacheKeyOfLfmStopIter = "MYPRJ_KV_LFM_REC-STOP_ITER" //是否停止下一次迭代， 空：否，非空：是

const cacheKeyOfLfmVisitorMaxWeight = "MYPRJ_SET_LFM_REC-VISITOR_MAX_WEIGHT" //访问者最大权重
const cacheKeyOfLfmSpuMaxWeight = "MYPRJ_SET_LFM_REC-SPU_MAX_WEIGHT"         //SPU最大权重
const cacheKeyOfLfmMaxPredict = "MYPRJ_SET_LFM_REC-SPU_MAX_PREDICT"          //Predict最大值

const cacheKeyOfLfmVisitor = "MYPRJ_SET_LFM_REC-VISITOR"                                 //操作（浏览、加入购物袋、收藏、下单）商品的访问者
const offLineCacheKeyOfLfmVisitor = "MYPRJ_HASH_LFM_REC_OFFLINE_VISITOR_SAMPLE"          //访问者样本
const offLineCacheKeyOfLfmVisitorSkip = "MYPRJ_HASH_LFM_REC_OFFLINE_VISITOR_SAMPLE_SKIP" //生成用户推荐时，需跳过的SPU（即下单的）
const cacheKeyOfLfmVisitorRec = "MYPRJ_SET_LFM_REC-VISITOR-REC"                          //操作（浏览、加入购物袋、收藏、下单）商品的访问者

const cacheKeyOfLfmVSpu = "MYPRJ_SET_LFM_REC-VSPU"                  //操作（浏览、加入购物袋、收藏、下单）的商品
const offLineCacheKeyOfLfmVSpu = "MYPRJ_HASH_LFM_REC_OFFLINE_VSPUS" //SPU离线数据

const cacheKeyOfLfmPMatrix = "MYPRJ_HASH_LFM_REC_OFFLINE-P_MATRIX"                  //P_MATRIX
const cacheKeyOfLfmQMatrix = "MYPRJ_HASH_LFM_REC_OFFLINE-Q_MATRIX"                  //Q_MATRIX
const cacheKeyOfLfmPMatrixFinal = "MYPRJ_HASH_LFM_REC_OFFLINE-P_MATRIX_FINAL"       //P_MATRIX 最终保存
const cacheKeyOfLfmQMatrixFinal = "MYPRJ_HASH_LFM_REC_OFFLINE-Q_MATRIX_FINAL"       //Q_MATRIX 最终保存
const cacheKeyOfLfmQtMatrixFinal = "MYPRJ_HASH_LFM_REC_OFFLINE-QT_MATRIX_FINAL"     //QT_MATRIX 最终保存
const cacheKeyOfLfmQtFeaturesFinal = "MYPRJ_HASH_LFM_REC_OFFLINE-QT_FEATURES_FINAL" //Q_MATRIX 最终保存

const offLineCacheKeyOfLfmUiSample = "MYPRJ_HASH_LFM_REC_OFFLINE_UI_SAMPLE" //访问者样本（正负集合）离线数据

const cachePreKeyOfLfmMemberRec = "MYPRJ_REC-LFM-MEMBER-" //LFM推荐给用户商品的缓存的键的前缀

const cachePreKeyOfLfmSpuLock = "MYPRJ_KV_LFM_REC-SPU-LOCK-" //当某个线程对一个SPU计算时需加锁
const cacheKeyOfAlpha = "MYPRJ_KV_LFM_REC_ALPHA"

//定义配置参数
var cfg config
var visitorMaxWeight float64 = 0
var lfmStopIter bool = false

//初始化配置参数，从数据库读取自定义配置
func init() {
	cfg = config{
		analysisDays:                15,
		cacheSkuNum:                 80,
		cacheExpire:                 2592000,
		processNum:                  5,
		lfmProcessNum:               10,
		sampleVisitorMaxNum:         100,
		sampleSkuMaxNum:             30000,
		positiveSampleMinNum:        10,
		offLineDataExpire:           2592000,
		classCount:                  10,
		iterCount:                   11,
		iterStep:                    0.9,
		alpha:                       0.0004,
		lamda:                       0.01,
		multipleOfMaxWeight4Predict: 1,
		weightOfNegativeItem:        5,
		actionWeight:                "{\"click\":1,\"fav\":8,\"cart\":15,\"order\":50}",
	}
	cfg.getFromCache()
}

//获取操作商品的权重设置
func getActionWeight() map[string]int64 {
	ret := make(map[string]int64, 0)
	ret["click"] = gjson.Get(cfg.actionWeight, "click").Int()
	ret["fav"] = gjson.Get(cfg.actionWeight, "fav").Int()
	ret["cart"] = gjson.Get(cfg.actionWeight, "cart").Int()
	ret["order"] = gjson.Get(cfg.actionWeight, "order").Int()
	return ret
}

//SQL计算权重的字段
func fieldExprOfCountWeight() string {
	actWeight := getActionWeight()
	weights := fmt.Sprintf("ROUND(SUM(click_num * %d + fav_num * %d + order_num * %d)+SQRT(10*SUM(cart_num * %d))) AS weights", actWeight["click"], actWeight["fav"], actWeight["order"], actWeight["cart"])
	return weights
}
