/*
推荐系统程序
支持LFM模型(Latent Factor Mode，潜在因子算法)、COS相似推荐算法（Todo）
执行方法：recommend [lfm|cos]
@author peaksoho
*/

package main

import (
	"fmt"
	"os"
	"recommend/cos"
	"recommend/lfm"
	"recommend/server"
	"runtime"
)

var algorithm string

func init() {
	if len(os.Args) < 2 {
		fmt.Println("[Warn] Please specify the algorithm to run: Command [lfm | cos]\n")
		os.Exit(1)
	} else {
		algorithm = os.Args[1]
	}
}

func main() {
	//fmt.Println(os.Args)

	defer server.RedisPool.Close()
	defer server.RecommendDbW.Close()

	runtime.GOMAXPROCS(runtime.NumCPU() - 1)

	switch algorithm {
	case "lfm":
		isParallel := false
		if len(os.Args) == 3 && os.Args[2] == "mp" {
			isParallel = true
		}
		lfm.Run(isParallel)
	case "cos":
		cos.Run()
	default:
		fmt.Println("[Warn] Please specify the algorithm to run: Command [lfm | cos]\n")
	}

}
