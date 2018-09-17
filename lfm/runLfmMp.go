package lfm

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"recommend/server"
	"recommend/util"
	"strconv"
	"time"

	"github.com/garyburd/redigo/redis"
)

//执行LFM算法
func runLfmMp() {
	start := time.Now() //记录开始时间
	log.Println("[INFO][runLfm]开始LFM分析...")

	var chanVisitor chan string

	fmt.Println("IterCount:")

	alpha := cfg.alpha
	for i := int64(1); i <= cfg.iterCount; i++ {
		var maxPredict float64 = 0.0
		fmt.Println("No.", i, ", alpha: ", alpha)
		subLfmMp(chanVisitor, alpha, &maxPredict)
		fmt.Println("----- maxPredict: ", maxPredict, "-----")
		alpha = alpha * cfg.iterStep
		if lfmStopIter {
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
func subLfmMp(chV chan string, alpha float64, mPt *float64) {
	chV = make(chan string, cfg.lfmProcessNum)
	chanDone := make(chan int64, cfg.lfmProcessNum)         //用于计算处理完成visitor数
	chanMaxPredict := make(chan float64, cfg.lfmProcessNum) //用于计算最大兴趣度

	redisTmp0 := server.GetRedis("tmp")
	defer redisTmp0.Close()

	//计算最大操作兴趣度
	go func(ch chan float64) {
		for true {
			p, isExists := <-ch
			if !isExists {
				break
			}
			if *mPt < p {
				*mPt = p
			}
		}
	}(chanMaxPredict)

	//获取样本中的访问者
	needDoNum := getVisitors(chV, redisTmp0)
	if needDoNum == 0 {
		log.Println("[ERROR][subLfm]", "未获取到采样访问者！")
		os.Exit(1)
	}

	//计算处理完成visitor数
	go util.ShowProgress(chanDone, needDoNum, 500)

	util.MultiThreadExec(cfg.lfmProcessNum, func() {
		redisTmp := server.GetRedis("tmp")
		defer redisTmp.Close()

		for true {
			visitor := <-chV
			if visitor == "" {
				break
			}

			sample := getUSampleInUISample(redisTmp, visitor)
			pVisitorVal := getPMatrixVisitorVal(redisTmp, visitor, false)

			for spu, rui := range sample {
				var isLock int64 = 0
				lockKey := cachePreKeyOfLfmSpuLock + spu
				lckStart := time.Now() //记录开始时间
				for true {
					lckV, err := redisTmp.Do("GET", lockKey)
					if err != nil {
						log.Println("[ERROR][subLfm]1", err)
					}
					if lckV != nil {
						isLock, err = redis.Int64(lckV, nil)
						if err != nil {
							log.Println("[ERROR][subLfm]2", err)
						}
					}
					if isLock == 0 {
						_, err = redisTmp.Do("SET", lockKey, 1)
						if err != nil {
							log.Println("[ERROR][subLfm]3", err)
						}
						_, err = redisTmp.Do("EXPIRE", lockKey, 2)
						if err != nil {
							log.Println("[ERROR][subLfm]3", err)
						}
						break
					}
					time.Sleep(2 * time.Millisecond)
					lckEnd := time.Now()                                        //记录结束时间
					lckDiffTime := lckEnd.Sub(lckStart).Nanoseconds() / 1000000 //输出执行时间，单位为毫秒
					if lckDiffTime > 10 {                                       //超时，退出等待
						break
					}
				}

				qSpuVal := getQMatrixSpuVal(redisTmp, spu, false)
				if qSpuVal == nil {
					log.Println("[ERROR][subLfm][SPU:", spu, "]缺少QMatrix")
					return
				}

				pui := predict(pVisitorVal, qSpuVal, false)
				chanMaxPredict <- pui

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
				_, err = redisTmp.Do("HSET", cacheKeyOfLfmQMatrix, spu, string(js))
				if err != nil {
					log.Println("[ERROR][subLfm]4", err)
				}

				_, err = redisTmp.Do("DEL", lockKey)
				if err != nil {
					log.Println("[ERROR][subLfm]5", err)
				}
			}

			js, err := json.Marshal(pVisitorVal)
			if err != nil {
				log.Println("[ERROR][subLfm]VISITOR:", visitor, ", json.Marshal failed:", err, "\n pVisitorVal: ", pVisitorVal)
				return
			}
			_, err = redisTmp.Do("HSET", cacheKeyOfLfmPMatrix, visitor, string(js))
			if err != nil {
				log.Println("[ERROR][subLfm]5", err)
			}

			chanDone <- 1
		}
	})

	close(chanDone)
	close(chanMaxPredict)
}
