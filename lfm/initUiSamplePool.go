package lfm

import (
	"encoding/json"

	"log"
	"math/rand"
	"os"
	"recommend/server"
	"recommend/util"
	"time"

	"github.com/garyburd/redigo/redis"
)

//获取样本中的访问者
func getVisitors(ch chan string, rds redis.Conn) int64 {
	visitors, err := redis.MultiBulk(rds.Do("HKEYS", offLineCacheKeyOfLfmVisitor))
	if err != nil {
		log.Println("[ERROR][initUiSamplePool]", err)
	}
	go func(data []interface{}) {
		for _, vt := range data {
			vtor, err := redis.String(vt, nil)
			if err != nil {
				log.Println("[ERROR][initUiSamplePool]", err)
			}
			ch <- vtor
		}
		close(ch)
	}(visitors)
	return int64(len(visitors))
}

/**
 * 获取用户正反馈物品：用户操作过的物品
 * @param string visitor 访问者
 * @param int retMinNum //返回正反馈SPU个数 0不限
 * @return map 正反馈{spu:weight}
 */
func getVisitorPositiveItem(rds redis.Conn, visitor string, retMinNum int64) map[string]float64 {
	js, err := redis.String(rds.Do("HGET", offLineCacheKeyOfLfmVisitor, visitor))
	if err != nil {
		log.Println("[ERROR][initUiSamplePool]", err)
	}
	spuWeights := make(map[string]float64, 0)
	json.Unmarshal([]byte(js), &spuWeights)
	return spuWeights
}

func getVSpu(rds redis.Conn) []string {
	data, err := redis.MultiBulk(rds.Do("HKEYS", offLineCacheKeyOfLfmVSpu))
	if err != nil {
		log.Println("[ERROR][getVSpu]", err)
	}
	var vSpus []string
	for _, v := range data {
		spu, err := redis.String(v, nil)
		if err != nil {
			log.Println("[ERROR][getVSpu]", err)
		}
		vSpus = append(vSpus, spu)
	}
	return vSpus
}

/**
 * 获取用户负反馈物品：热门但是用户没有进行过操作 与正反馈数量相等
 * @param string visitor 访问者
 * @param map positiveItem 正反馈SPU
 * @return map 负反馈SPU{spu:weight}
 */
func getVisitorNegativeItem(vSpus []string, positiveItem map[string]float64) map[string]float64 {
	var diffSpus []string
	for _, spu := range vSpus {
		if _, ok := positiveItem[spu]; !ok {
			diffSpus = append(diffSpus, spu)
		}
	}
	pNum := len(positiveItem)
	sNum := len(diffSpus)
	offset := 0
	if sNum > pNum {
		offset = rand.Intn(sNum - pNum)
	}
	nSpus := diffSpus[offset : offset+pNum]
	negativeItem := make(map[string]float64, 0)
	for _, nSpu := range nSpus {
		if cfg.weightOfNegativeItem <= 0 {
			negativeItem[nSpu] = float64(cfg.weightOfNegativeItem)
		} else if cfg.weightOfNegativeItem > 1 {
			negativeItem[nSpu] = -float64(rand.Intn(int(cfg.weightOfNegativeItem)))
			if negativeItem[nSpu] == 0 {
				negativeItem[nSpu] = -1.0
			}
		} else {
			negativeItem[nSpu] = -1.0
		}
	}
	return negativeItem
}

//初始化样本数据
func initUiSamplePool() {
	start := time.Now() //记录开始时间
	chanVisitor := make(chan string, cfg.processNum)
	chanDone := make(chan int64, cfg.processNum) //用于计算处理完成visitor数

	redisTmp0 := server.GetRedis("tmp")
	defer redisTmp0.Close()

	//删除原数据缓存
	_, err := redisTmp0.Do("DEL", offLineCacheKeyOfLfmUiSample)
	if err != nil {
		log.Println("[ERROR][initUiSamplePool]", err)
	}
	//获取样本中的访问者
	needDoNum := getVisitors(chanVisitor, redisTmp0)
	if needDoNum == 0 {
		log.Println("[ERROR][initUiSamplePool]", "未获取到采样访问者！")
		os.Exit(1)
	}
	log.Println("[INFO][initUiSamplePool]", "需处理的访问者有", needDoNum, "个")

	//计算处理完成visitor数
	go util.ShowProgress(chanDone, needDoNum, 500)

	vSpus := getVSpu(redisTmp0)

	//处理每个访问者的正负样本
	util.MultiThreadExec(cfg.processNum, func() {
		redisTmp := server.GetRedis("tmp")
		defer redisTmp.Close()

		for true {
			visitor := <-chanVisitor
			if visitor == "" {
				break
			}
			itemDict := make(map[string]float64, 0)
			positiveItem := getVisitorPositiveItem(redisTmp, visitor, cfg.positiveSampleMinNum)
			//fmt.Println(positiveItem)
			if len(positiveItem) == 0 {
				log.Println("[WARN][initUiSamplePool]", "[VISITOR:", visitor, "]正集合样本生成失败！")
				break
			}
			negativeItem := getVisitorNegativeItem(vSpus, positiveItem)
			//fmt.Println(negativeItem)
			if len(negativeItem) == 0 {
				log.Println("[WARN][initUiSamplePool]", "[VISITOR:", visitor, "]负集合样本生成失败！")
				break
			}
			for spu, w := range positiveItem {
				itemDict[spu] = w
			}
			for spu, w := range negativeItem {
				itemDict[spu] = w
			}

			js, err := json.Marshal(itemDict)
			if err != nil {
				log.Println("[ERROR][initUiSamplePool]VISITOR:", visitor, ", json.Marshal failed:", err)
				return
			}
			_, err = redisTmp.Do("HSET", offLineCacheKeyOfLfmUiSample, visitor, string(js))
			if err != nil {
				log.Println("[ERROR][initUiSamplePool]", err)
			}

			chanDone <- 1
		}
	})

	close(chanDone)

	_, err = redisTmp0.Do("EXPIRE", offLineCacheKeyOfLfmUiSample, cfg.offLineDataExpire)
	if err != nil {
		log.Println("[ERROR][initUiSamplePool]", err)
	}

	time.Sleep(10 * time.Millisecond)
	end := time.Now()                                  //记录结束时间
	diffTime := end.Sub(start).Nanoseconds() / 1000000 //输出执行时间，单位为毫秒
	log.Println("[INFO][initUiSamplePool]", "初始化访问者样本（正负集合）离线数据完成，使用时间：", diffTime, "ms\n")
}
