package lfm

import (
	"encoding/json"
	"log"
	"math"
	"math/rand"
	"os"
	"recommend/server"
	"recommend/util"
	"time"
)

//初始化PQ矩阵
func initPQMatrix() {
	start := time.Now() //记录开始时间

	redisTmp0 := server.GetRedis("tmp")
	defer redisTmp0.Close()

	//删除原数据缓存
	_, err := redisTmp0.Do("DEL", cacheKeyOfLfmPMatrix)
	if err != nil {
		log.Println("[ERROR][initPQMatrix]", err)
	}
	_, err = redisTmp0.Do("DEL", cacheKeyOfLfmQMatrix)
	if err != nil {
		log.Println("[ERROR][initPQMatrix]", err)
	}

	//------------------- 生成P矩阵 -------------------
	chanVisitor := make(chan string, cfg.processNum)
	chanDone := make(chan int64, cfg.processNum) //用于计算处理完成visitor数
	//获取样本中的访问者
	needDoNum := getVisitors(chanVisitor, redisTmp0)
	if needDoNum == 0 {
		log.Println("[ERROR][initPMatrix]", "未获取到采样访问者！")
		os.Exit(1)
	}
	log.Println("[INFO][initPMatrix]开始生成P矩阵，需处理的访问者有", needDoNum, "个...")

	//计算处理完成visitor数
	go util.ShowProgress(chanDone, needDoNum, 500)

	util.MultiThreadExec(cfg.processNum, func() {
		redisTmp := server.GetRedis("tmp")
		defer redisTmp.Close()

		for true {
			visitor := <-chanVisitor
			if visitor == "" {
				break
			}

			pMRowList := make(map[int64]float64, 0)
			for i := int64(1); i <= cfg.classCount; i++ {
				pMRowList[i] = rand.Float64() / math.Sqrt(float64(cfg.classCount))
			}
			js, err := json.Marshal(pMRowList)
			if err != nil {
				log.Println("[ERROR][initPMatrix]VISITOR:", visitor, ", json.Marshal failed:", err)
				return
			}
			_, err = redisTmp.Do("HSET", cacheKeyOfLfmPMatrix, visitor, string(js))
			if err != nil {
				log.Println("[ERROR][initPMatrix]", err)
			}
			chanDone <- 1
		}
	})

	close(chanDone)

	_, err = redisTmp0.Do("EXPIRE", cacheKeyOfLfmPMatrix, cfg.offLineDataExpire)
	if err != nil {
		log.Println("[ERROR][initPMatrix]", err)
	}

	//------------------- 生成Q矩阵 -------------------
	chanSpu := make(chan string, cfg.processNum)
	chanDone = make(chan int64, cfg.processNum) //用于计算处理完成Spu数
	vSpus := getVSpu(redisTmp0)
	needDoNum = int64(len(vSpus))
	if needDoNum == 0 {
		log.Println("[ERROR][initQMatrix]", "未获取到采样SPU！")
		os.Exit(1)
	}
	log.Println("[INFO][initQMatrix]开始生成Q矩阵，需处理的SPU有", needDoNum, "个...")

	go func(ch chan string) {
		for _, spu := range vSpus {
			ch <- spu
		}
		close(ch)
	}(chanSpu)

	//计算处理完成Spu数
	go util.ShowProgress(chanDone, needDoNum, 2000)

	util.MultiThreadExec(cfg.processNum, func() {
		redisTmp := server.GetRedis("tmp")
		defer redisTmp.Close()

		for true {
			spu := <-chanSpu
			if spu == "" {
				break
			}

			qMRowList := make(map[int64]float64, 0)
			for i := int64(1); i <= cfg.classCount; i++ {
				qMRowList[i] = rand.Float64() / math.Sqrt(float64(cfg.classCount))
			}
			js, err := json.Marshal(qMRowList)
			if err != nil {
				log.Println("[ERROR][initQMatrix]SPU:", spu, ", json.Marshal failed:", err)
				return
			}
			_, err = redisTmp.Do("HSET", cacheKeyOfLfmQMatrix, spu, string(js))
			if err != nil {
				log.Println("[ERROR][initQMatrix]", err)
			}
			chanDone <- 1
		}
	})

	close(chanDone)

	_, err = redisTmp0.Do("EXPIRE", cacheKeyOfLfmQMatrix, cfg.offLineDataExpire)
	if err != nil {
		log.Println("[ERROR][initQMatrix]", err)
	}

	time.Sleep(10 * time.Millisecond)
	end := time.Now()                                  //记录结束时间
	diffTime := end.Sub(start).Nanoseconds() / 1000000 //输出执行时间，单位为毫秒
	log.Println("[INFO][initPQMatrix]", "PQ矩阵已生成，使用时间：", diffTime, "ms")
}
