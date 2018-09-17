//实例化Redis、MySql
package server

import (
	"log"
	"os"
	"path/filepath"

	"github.com/garyburd/redigo/redis"
)

var (
	ServerCfg    Config
	RecommendDbW MysqlConn
)

func init() {
	ServerCfg = Config{}
	execDir := filepath.Dir(os.Args[0])
	ServerCfg.ReadJsonFile(execDir + "/conf/StorageCfg.json")

	//初始化redis连接池
	RedisPool = InitRedisPool(ServerCfg.Get("redis.tmp").String())
	CfgRedisPool = InitRedisPool(ServerCfg.Get("redis.nosql").String())

	RecommendDbW = GetMysql("recommend", "w")
}

//从Redis连接池获取Redis连接
func GetRedis(index string) redis.Conn {
	var c redis.Conn
	switch index {
	case "tmp":
		fallthrough
	case "recommend":
		c = RedisPool.Get()
	case "nosql":
		c = CfgRedisPool.Get()
	}
	db := ServerCfg.Get("redis." + index + ".db").Int()

	if _, err := c.Do("SELECT", db); err != nil {
		c.Close()
		log.Fatalln("Redis: ", index, ", DB: ", db, err)
	}
	return c
}

///从MySql连接池获取MySql连接 Todo
func GetMysql(dbname string, rw string) MysqlConn {
	rwType := "master"
	if rw == "r" {
		rwType = "slaver"
	}
	label := "mysql." + dbname + "." + rwType

	var conn MysqlConn
	var ok bool
	if conn.db, ok = mysqlDbMap[label]; !ok {
		mysqlConnMutex.Lock()
		if conn.db, ok = mysqlDbMap[label]; !ok {
			conn.db = getMysqlConn(ServerCfg.Get(label).String())
			mysqlDbMap[label] = conn.db
		}
	}
	mysqlConnMutex.Unlock()
	log.Println("DB connection ok: ", label)
	return conn
}
