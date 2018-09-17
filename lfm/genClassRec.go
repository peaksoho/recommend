package lfm

import (
	"encoding/json"
	"log"
	"recommend/server"
	"recommend/util"
	"time"

	"github.com/garyburd/redigo/redis"
)

//生成自定义类型推荐数据
func genClassRec() {
	start := time.Now() //记录开始时间

	redisTmp := server.GetRedis("tmp")
	defer redisTmp.Close()

	_, err := redisTmp.Do("DEL", cacheKeyOfLfmQtMatrixFinal)
	if err != nil {
		log.Println("[ERROR][genClassRec]", err)
	}

	qT := make(map[string]map[string]float64, 0)
	spus, err := redis.MultiBulk(redisTmp.Do("HKEYS", cacheKeyOfLfmQMatrixFinal))
	if err != nil {
		log.Println("[ERROR][genClassRec]", err)
	}
	for _, vs := range spus {
		spu, err := redis.String(vs, nil)
		if err != nil {
			log.Println("[ERROR][genClassRec]", err)
		}
		qSpuVal := getQMatrixSpuVal(redisTmp, spu, true)

		for f, v := range qSpuVal {
			if qT[f] == nil {
				qT[f] = make(map[string]float64, 0)
			}
			qT[f][spu] = v
		}
	}
	for f, vm := range qT {
		r := util.SortMapByValue(vm, "DESC")
		var sf util.SfPairList
		if int64(len(r)) > cfg.cacheSkuNum {
			sf = r[0:cfg.cacheSkuNum]
		} else {
			sf = r
		}

		spuWeights := make(map[string]float64, 0)

		for _, vp := range sf {
			spuWeights[vp.Key] = vp.Value
		}

		js, err := json.Marshal(spuWeights)
		if err != nil {
			log.Println("[ERROR][genClassRec]Class:", f, ", json.Marshal failed:", err)
			return
		}
		_, err = redisTmp.Do("HSET", cacheKeyOfLfmQtMatrixFinal, f, string(js))
		if err != nil {
			log.Println("[ERROR][genClassRec]Class:", f, ", ERR:", err)
		}
	}
	_, err = redisTmp.Do("EXPIRE", cacheKeyOfLfmQtMatrixFinal, cfg.offLineDataExpire)
	if err != nil {
		log.Println("[ERROR][genClassRec]", err)
	}

	time.Sleep(10 * time.Millisecond)
	end := time.Now()                                  //记录结束时间
	diffTime := end.Sub(start).Nanoseconds() / 1000000 //输出执行时间，单位为毫秒
	log.Println("[INFO][genClassRec]", "生成自定义类型推荐数据完成，使用时间：", diffTime, "ms\n")
}
