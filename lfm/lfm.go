/*
LFM算法实现
@author peaksoho
*/
package lfm

import (
	"log"
	"time"
)

func Run(lfmIsParallel bool) {
	log.Println("LFM algorithm analysis is starting...")
	log.Printf("LFM Config: %v\n", cfg)
	//记录开始时间
	start := time.Now()

	initVisitor()      //初始化访问者离线数据
	initUiSamplePool() //初始化样本数据
	initPQMatrix()     //初始化PQ矩阵
	if lfmIsParallel { //执行LFM算法
		runLfmMp()
	} else {
		runLfm()
	}
	genClassRec()   //生成自定义类型推荐数据
	genVisitorRec() //生成用户推荐数据

	//记录结束时间
	end := time.Now()
	//输出执行时间，单位为毫秒
	diffTime := end.Sub(start).Nanoseconds() / 1000000
	log.Println("LFM推荐算法分析完成，使用时间: ", diffTime, "ms")
}
