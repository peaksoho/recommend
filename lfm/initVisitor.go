package lfm

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"recommend/server"
	"recommend/util"
	"strconv"
	"strings"
	"time"
)

//获取指定天内的访问者，存入redis，返回访问者数量
func saddVisitorsInDays(ch chan string) int64 {
	tSql := fmt.Sprintf("SELECT visitor FROM pms_product_visit_record WHERE date>='%s' GROUP BY visitor HAVING COUNT(DISTINCT spu)>%d ORDER BY SUM(weight) DESC LIMIT %d",
		util.DateBeforeDays(cfg.analysisDays),
		cfg.positiveSampleMinNum,
		cfg.sampleVisitorMaxNum)
	rows, _ := server.RecommendDbW.FetchRows(tSql)
	go func(dataList *[]map[string]string) {
		for _, v := range *dataList {
			ch <- v["visitor"]
		}
		close(ch)
	}(rows)
	return int64(len(*rows))
}

//初始化访问者离线数据
func initVisitor() {
	//记录开始时间
	start := time.Now()
	log.Printf("[INFO][initVisitro]开始查询访问者，分析%d天的访问数据...\n", cfg.analysisDays)

	chanSpuVisitWeight := make(chan float64, cfg.processNum) //用于计算最大操作权重
	chanDone := make(chan int64, cfg.processNum)             //用于计算处理完成visitor数
	chanVisitor := make(chan string, cfg.processNum)

	needDoNum := saddVisitorsInDays(chanVisitor)
	if needDoNum == 0 {
		log.Println("[ERROR][initVisitor]", "未获取到访问者！")
		os.Exit(1)
	}
	//记录结束时间
	end := time.Now()
	//输出执行时间，单位为毫秒
	diffTime := end.Sub(start).Nanoseconds() / 1000000
	log.Println("[INFO][initVisitro]", "查询访问者结束，使用时间：", diffTime, "ms，需处理的访问者有", needDoNum, "个")

	redisTmp0 := server.GetRedis("tmp")
	defer redisTmp0.Close()

	_, err := redisTmp0.Do("DEL", offLineCacheKeyOfLfmVSpu)
	if err != nil {
		log.Println("[ERROR][initVisitro]", err)
	}
	_, err = redisTmp0.Do("DEL", offLineCacheKeyOfLfmVisitor)
	if err != nil {
		log.Println("[ERROR][initVisitro]", err)
	}

	//计算最大操作权重
	go func(ch chan float64) {
		for true {
			w, isExists := <-ch
			if !isExists {
				break
			}
			if visitorMaxWeight < w {
				visitorMaxWeight = w
			}
		}
	}(chanSpuVisitWeight)
	//计算处理完成visitor数
	go util.ShowProgress(chanDone, needDoNum, 500)

	//获取每个访问者的访问SPU及权重
	weights := fieldExprOfCountWeight()

	util.MultiThreadExec(cfg.processNum, func() {
		redisTmp := server.GetRedis("tmp")
		defer redisTmp.Close()

		for true {
			visitor := <-chanVisitor
			if visitor == "" {
				break
			}
			//fmt.Println(visitor)
			tSql := fmt.Sprintf("SELECT spu,SUM(order_num) AS order_nums,%s FROM pms_product_visit_record WHERE date>='%s' AND visitor='%s' GROUP BY spu ORDER BY weights DESC",
				weights,
				util.DateBeforeDays(cfg.analysisDays),
				visitor)
			rows, _ := server.RecommendDbW.FetchRows(tSql)
			num := int64(len(*rows))
			if num == 0 || num < cfg.positiveSampleMinNum {
				log.Println("[WARN][initVisitro]VISITOR:"+visitor+"] 访问SPU数量不足 ", cfg.positiveSampleMinNum, " 个")
				continue
			}
			spuWeights := make(map[string]float64, 0)
			var skipSpus []string
			for _, spuWeight := range *rows {
				_, err = redisTmp.Do("HINCRBY", offLineCacheKeyOfLfmVSpu, spuWeight["spu"], spuWeight["weights"]) //操作（浏览、加入购物袋、收藏、下单）的商品
				if err != nil {
					log.Println("[ERROR][initVisitro]", err)
				}

				spuWeights[spuWeight["spu"]], err = strconv.ParseFloat(spuWeight["weights"], 64) //访问者样本
				if err != nil {
					log.Println("[ERROR][initVisitro]", err)
				}

				chanSpuVisitWeight <- spuWeights[spuWeight["spu"]] //计算最大操作权重

				haveOrder, err := strconv.ParseInt(spuWeight["order_nums"], 10, 64)
				if err != nil {
					log.Println("[ERROR][initVisitro]", err)
				}
				if haveOrder > 0 {
					skipSpus = append(skipSpus, spuWeight["spu"])
				}
			}
			//访问者样本
			js, err := json.Marshal(spuWeights)
			if err != nil {
				log.Println("[ERROR][initVisitro]VISITOR:", visitor, ", json.Marshal failed:", err)
				return
			}
			_, err = redisTmp.Do("HSET", offLineCacheKeyOfLfmVisitor, visitor, string(js))
			if err != nil {
				log.Println("[ERROR][initVisitro]", err)
			}
			//生成用户推荐时，需跳过的SPU（即下单的）
			skipSpusStr := strings.Join(skipSpus, ",")
			if skipSpusStr != "" {
				_, err = redisTmp.Do("HSET", offLineCacheKeyOfLfmVisitorSkip, visitor, skipSpusStr)
				if err != nil {
					log.Println("[ERROR][initVisitro]", err)
				}
			}
			chanDone <- 1
		}
	})

	close(chanSpuVisitWeight)
	close(chanDone)
	time.Sleep(10 * time.Millisecond)

	_, err = redisTmp0.Do("EXPIRE", offLineCacheKeyOfLfmVSpu, cfg.offLineDataExpire)
	if err != nil {
		log.Println("[ERROR][initVisitro]", err)
	}
	_, err = redisTmp0.Do("EXPIRE", offLineCacheKeyOfLfmVisitor, cfg.offLineDataExpire)
	if err != nil {
		log.Println("[ERROR][initVisitro]", err)
	}

	spuNum, err := redisTmp0.Do("HLEN", offLineCacheKeyOfLfmVSpu)
	if err != nil {
		log.Println("[ERROR][initVisitro]", err)
	}

	end = time.Now()                                  //记录结束时间
	diffTime = end.Sub(start).Nanoseconds() / 1000000 //输出执行时间，单位为毫秒
	log.Println("[INFO][initVisitro]", "分析访问者完成，使用时间：", diffTime, "ms，共成功处理", needDoNum, "个访问者的访问数据，SPU离线数据有", spuNum, "个，最大操作权重：", visitorMaxWeight, "\n")

}
