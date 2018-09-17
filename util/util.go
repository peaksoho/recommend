/*
公共函数
@author peak
@date 2018-03-23
*/
package util

import (
	"fmt"
	"io/ioutil"
	"log"
	"math"
	"os"
	"runtime"
	"sync"
	"time"
)

func ReadFile(filePth string) (string, error) {
	f, err := os.Open(filePth)
	if err != nil {
		return "", err
	}
	defer f.Close()
	fd, err := ioutil.ReadAll(f)
	return string(fd), err
}

//计算指定days天前的日期并返回
func DateBeforeDays(days int64) string {
	timestamp := time.Now().Unix()
	return time.Unix(timestamp-(86400*days), 0).Format("2006-01-02")
}

//计算进度
//@param ch 管道
//@param needDo 需完成的
//@param pNum 每完成pNum个输出一次进度
func ShowProgress(ch chan int64, needDo int64, pNum int64) {
	var done int64 = 0
	var tmp int64 = 0
	for true {
		n, isExists := <-ch
		if !isExists {
			if tmp > 0 {
				fmt.Print("..Done\n")
			}
			break
		}
		done = done + n
		tmp2 := math.Ceil((float64(done) - float64(tmp)) / float64(pNum))
		if tmp2 > 1 {
			tmp = done
			fmt.Printf("..%.2f%%", float64(done)*100.0/float64(needDo))
		}
	}
}

//多线程执行
//@param pNum 指定线程数
//@param fn 需执行的函数
func MultiThreadExec(pNum int64, fn func()) {
	var wg sync.WaitGroup
	var i int64
	for i = 0; i < pNum; i++ {
		wg.Add(1)
		go func(j int64) {
			fn()
			wg.Done()
		}(i)
	}
	log.Printf("NumGoroutine: %d.\n", runtime.NumGoroutine())
	wg.Wait() //等待这些goroutine执行结束
}

func InArrayString(str string, arr []string) bool {
	for _, v := range arr {
		if str == v {
			return true
		}
	}
	return false
}
