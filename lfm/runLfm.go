package lfm

import (
	"encoding/json"
	"fmt"
	"log"
	"recommend/server"
	"strconv"
	"time"

	"github.com/garyburd/redigo/redis"
)

//执行LFM算法
func runLfm() {
	start := time.Now() //记录开始时间
	log.Println("[INFO][runLfm]开始LFM分析...")

	redisTmp := server.GetRedis("tmp")
	defer redisTmp.Close()

	visitors, err := redis.MultiBulk(redisTmp.Do("HKEYS", offLineCacheKeyOfLfmVisitor))
	if err != nil {
		log.Println("[ERROR][runLfm]", err)
	}

	fmt.Println("IterCount:")

	alpha := cfg.alpha
	for i := int64(1); i <= cfg.iterCount; i++ {
		var maxPredict float64 = 0.0
		fmt.Println("No.", i, ", alpha: ", alpha)
		for _, vt := range visitors {
			visitor, err := redis.String(vt, nil)
			if err != nil {
				log.Println("[ERROR][runLfm]", err)
			}
			subLfm(redisTmp, visitor, alpha, &maxPredict)
		}
		fmt.Println("----- maxPredict: ", maxPredict, "-----")
		alpha = alpha * cfg.iterStep
		if lfmStopIter {
			log.Println("[WARN][runLfm]", "达到退出条件")
			break
		}
	}

	genFinalMatrix()

	time.Sleep(10 * time.Millisecond)
	end := time.Now()                                  //记录结束时间
	diffTime := end.Sub(start).Nanoseconds() / 1000000 //输出执行时间，单位为毫秒
	log.Println("[INFO][runLfm]", "LFM分析完成，使用时间：", diffTime, "ms\n")

}

//每次迭代的LFM分析
func subLfm(rds redis.Conn, visitor string, alpha float64, mPt *float64) {
	sample := getUSampleInUISample(rds, visitor)
	pVisitorVal := getPMatrixVisitorVal(rds, visitor, false)

	for spu, rui := range sample {
		qSpuVal := getQMatrixSpuVal(rds, spu, false)
		if qSpuVal == nil {
			log.Println("[ERROR][subLfm][SPU:", spu, "]缺少QMatrix")
			return
		}

		pui := predict(pVisitorVal, qSpuVal, false)
		if pui > *mPt {
			*mPt = pui
		}
		eui := rui - pui

		for f := int64(1); f <= cfg.classCount; f++ {
			kf := strconv.Itoa(int(f))
			pVisitorVal[kf] += alpha * (eui*qSpuVal[kf] - cfg.lamda*pVisitorVal[kf])
			qSpuVal[kf] += alpha * (eui*pVisitorVal[kf] - cfg.lamda*qSpuVal[kf])
		}

		js, err := json.Marshal(qSpuVal)
		if err != nil {
			log.Println("[ERROR][subLfm]VISITOR:", spu, ", json.Marshal failed:", err, "\n qSpuVal: ", qSpuVal)
			return
		}
		_, err = rds.Do("HSET", cacheKeyOfLfmQMatrix, spu, string(js))
		if err != nil {
			log.Println("[ERROR][subLfm]4", err)
		}
	}

	js, err := json.Marshal(pVisitorVal)
	if err != nil {
		log.Println("[ERROR][subLfm]VISITOR:", visitor, ", json.Marshal failed:", err, "\n pVisitorVal: ", pVisitorVal)
		return
	}
	_, err = rds.Do("HSET", cacheKeyOfLfmPMatrix, visitor, string(js))
	if err != nil {
		log.Println("[ERROR][subLfm]5", err)
	}
}

/**
 * 获取样本中某个用户的集合
 * @param string visitor
 * @return array  //{1:0.212,2:3.019,...}
 */
func getUSampleInUISample(rds redis.Conn, visitor string) map[string]float64 {
	js, err := redis.String(rds.Do("HGET", offLineCacheKeyOfLfmUiSample, visitor))
	if err != nil {
		log.Println("[ERROR][subLfm][getUSampleInUISample]", err)
	}
	spuWeights := make(map[string]float64, 0)
	json.Unmarshal([]byte(js), &spuWeights)
	return spuWeights
}

/**
 * 获取P Matrix 中某个用户的数据
 * @param string visitor
 * @param bool useFinal 是否使用最终PQ矩阵，默认否
 * @return array  //{class:rate,...}
 */
func getPMatrixVisitorVal(rds redis.Conn, visitor string, useFinal bool) map[string]float64 {
	var err error
	var js string
	if useFinal {
		js, err = redis.String(rds.Do("HGET", cacheKeyOfLfmPMatrixFinal, visitor))
	} else {
		js, err = redis.String(rds.Do("HGET", cacheKeyOfLfmPMatrix, visitor))
	}
	if err != nil {
		log.Println("[ERROR][subLfm][getPMatrixVisitorVal]", err)
	}
	pMRowList := make(map[string]float64, 0)
	json.Unmarshal([]byte(js), &pMRowList)
	return pMRowList
}

/**
 * 获取Q Matrix 中某个SPU的数据
 * @param string spu
 * @param bool useFinal 是否使用最终PQ矩阵，默认否
 * @return array  //{class:rate,...}
 */
func getQMatrixSpuVal(rds redis.Conn, spu string, useFinal bool) map[string]float64 {
	var err error
	var js string
	if useFinal {
		js, err = redis.String(rds.Do("HGET", cacheKeyOfLfmQMatrixFinal, spu))
	} else {
		js, err = redis.String(rds.Do("HGET", cacheKeyOfLfmQMatrix, spu))
	}
	if err != nil {
		log.Println("[ERROR][subLfm][getQMatrixSpuVal]", err)
	}
	qMRowList := make(map[string]float64, 0)
	json.Unmarshal([]byte(js), &qMRowList)
	return qMRowList
}

/**
 * 利用参数p,q预测目标用户对目标物品的兴趣度
 * @param map[string]float64) pMRowList
 * @param map[string]float64) qMRowList
 * @return float 预测兴趣度
 */
func predict(pMRowList, qMRowList map[string]float64, useFinal bool) float64 {
	var res float64 = 0.0
	for f := int64(1); f <= cfg.classCount; f++ {
		kf := strconv.Itoa(int(f))
		_, ok1 := pMRowList[kf]
		_, ok2 := qMRowList[kf]

		if !ok1 || !ok2 {
			continue
		}
		res = res + pMRowList[kf]*qMRowList[kf]
	}
	if !useFinal && cfg.multipleOfMaxWeight4Predict > 0 && visitorMaxWeight > 0 {
		if res > cfg.multipleOfMaxWeight4Predict*visitorMaxWeight {
			lfmStopIter = true
		}
	}
	return res
}

//生成最终PQ矩阵缓存
func genFinalMatrix() {
	redisTmp := server.GetRedis("tmp")
	defer redisTmp.Close()

	_, err := redisTmp.Do("DEL", cacheKeyOfLfmPMatrixFinal)
	if err != nil {
		log.Println("[ERROR][genFinalMatrix]", err)
	}
	visitors, err := redis.MultiBulk(redisTmp.Do("HKEYS", cacheKeyOfLfmPMatrix))
	if err != nil {
		log.Println("[ERROR][genFinalMatrix]", err)
	}
	for _, vt := range visitors {
		visitor, err := redis.String(vt, nil)
		if err != nil {
			log.Println("[ERROR][genFinalMatrix]", err)
		}
		js, err := redis.String(redisTmp.Do("HGET", cacheKeyOfLfmPMatrix, visitor))
		if err != nil {
			log.Println("[ERROR][genFinalMatrix]", err)
		}
		_, err = redisTmp.Do("HSET", cacheKeyOfLfmPMatrixFinal, visitor, js)
		if err != nil {
			log.Println("[ERROR][genFinalMatrix]", err)
		}
	}
	_, err = redisTmp.Do("EXPIRE", cacheKeyOfLfmPMatrixFinal, cfg.offLineDataExpire)
	if err != nil {
		log.Println("[ERROR][genFinalMatrix]", err)
	}

	_, err = redisTmp.Do("DEL", cacheKeyOfLfmQMatrixFinal)
	if err != nil {
		log.Println("[ERROR][genFinalMatrix]", err)
	}
	spus, err := redis.MultiBulk(redisTmp.Do("HKEYS", cacheKeyOfLfmQMatrix))
	if err != nil {
		log.Println("[ERROR][genFinalMatrix]", err)
	}
	for _, vs := range spus {
		spu, err := redis.String(vs, nil)
		if err != nil {
			log.Println("[ERROR][genFinalMatrix]", err)
		}
		js, err := redis.String(redisTmp.Do("HGET", cacheKeyOfLfmQMatrix, spu))
		if err != nil {
			log.Println("[ERROR][genFinalMatrix]", err)
		}
		_, err = redisTmp.Do("HSET", cacheKeyOfLfmQMatrixFinal, spu, js)
		if err != nil {
			log.Println("[ERROR][genFinalMatrix]", err)
		}
	}
	_, err = redisTmp.Do("EXPIRE", cacheKeyOfLfmQMatrixFinal, cfg.offLineDataExpire)
	if err != nil {
		log.Println("[ERROR][genFinalMatrix]", err)
	}
}
