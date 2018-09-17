package lfm

import (
	"log"
	"os"
	"recommend/server"
	"recommend/util"
	"strings"
	"time"

	"github.com/garyburd/redigo/redis"
)

//生成用户推荐数据
func genVisitorRec() {
	start := time.Now() //记录开始时间

	chanVisitor := make(chan string, cfg.processNum)
	chanDone := make(chan int64, cfg.processNum) //用于计算处理完成visitor数

	redisTmp0 := server.GetRedis("tmp")
	defer redisTmp0.Close()

	//获取样本中的访问者
	needDoNum := getVisitors(chanVisitor, redisTmp0)
	if needDoNum == 0 {
		log.Println("[ERROR][genVisitorRec]", "未获取到采样访问者！")
		os.Exit(1)
	}
	log.Println("[INFO][genVisitorRec]", "需处理的访问者有", needDoNum, "个")

	//计算处理完成visitor数
	go util.ShowProgress(chanDone, needDoNum, 500)

	//生成用户推荐数据
	util.MultiThreadExec(cfg.processNum, func() {
		redisTmp := server.GetRedis("tmp")
		defer redisTmp.Close()

		for true {
			visitor := <-chanVisitor
			if visitor == "" {
				break
			}

			skipSpus := getSkipSpusOfVisitor(redisTmp, visitor)
			//fmt.Println("skipSpus: ", skipSpus)
			sample := getUSampleInUISample(redisTmp, visitor)
			pVisitorVal := getPMatrixVisitorVal(redisTmp, visitor, true)
			spuWeights := make(map[string]float64)
			for spu, _ := range sample {
				qSpuVal := getQMatrixSpuVal(redisTmp, spu, false)
				if qSpuVal == nil {
					log.Println("[ERROR][genVisitorRec][SPU:", spu, "]缺少QMatrix")
					return
				}

				pui := predict(pVisitorVal, qSpuVal, false)
				if !util.InArrayString(spu, skipSpus) && pui > 0 {
					spuWeights[spu] = pui
				}
			}

			r := util.SortMapByValue(spuWeights, "DESC")
			var sf util.SfPairList
			if int64(len(r)) > cfg.cacheSkuNum {
				sf = r[0:cfg.cacheSkuNum]
			} else {
				sf = r
			}
			var recSpus []string
			for _, vp := range sf {
				recSpus = append(recSpus, vp.Key)
			}

			//fmt.Println(sf)

			res := strings.Join(recSpus, ",")
			//fmt.Println(res)

			_, err := redisTmp.Do("SET", cachePreKeyOfLfmMemberRec+visitor, res)
			if err != nil {
				log.Println("[ERROR][genVisitorRec], ERR:", err)
			}

			chanDone <- 1
		}
	})

	close(chanDone)

	time.Sleep(10 * time.Millisecond)
	end := time.Now()                                  //记录结束时间
	diffTime := end.Sub(start).Nanoseconds() / 1000000 //输出执行时间，单位为毫秒
	log.Println("[INFO][genVisitorRec]", "生成用户推荐数据完成，使用时间：", diffTime, "ms\n")
}

//获取用户需跳过的SPU，返回spu数组
func getSkipSpusOfVisitor(rds redis.Conn, visitor string) []string {
	v, err := rds.Do("HGET", offLineCacheKeyOfLfmVisitorSkip, visitor)
	if err != nil {
		log.Println("[ERROR][genVisitorRec][getSkipSpusOfVisitor][VISITOR: ", visitor, "]", err)
	}
	if v != nil {
		str, _ := redis.String(v, nil)
		if str != "" {
			return strings.Split(str, ",")
		}
	}
	return nil
}
