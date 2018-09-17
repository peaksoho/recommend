package server

import (
	"log"
	"time"

	"github.com/garyburd/redigo/redis"
	"github.com/tidwall/gjson"
)

type RedisCfg struct {
	address     string
	password    string
	maxIdle     int
	maxActive   int
	idleTimeout int
	db          int64
}

var (
	CfgRedisPool redis.Pool
	RedisPool    redis.Pool
)

func newPool(redisConf RedisCfg) redis.Pool {
	return redis.Pool{
		MaxIdle:     redisConf.maxIdle,
		MaxActive:   redisConf.maxActive,
		IdleTimeout: (time.Duration(redisConf.idleTimeout) * time.Second),
		Dial: func() (redis.Conn, error) {
			c, err := redis.Dial("tcp", redisConf.address)
			if err != nil {
				return nil, err
			}
			if redisConf.password != "" {
				if _, err := c.Do("AUTH", redisConf.password); err != nil {
					c.Close()
					return nil, err
				}
			}
			if _, err := c.Do("SELECT", redisConf.db); err != nil {
				c.Close()
				return nil, err
			}
			return c, err
		},
		TestOnBorrow: func(c redis.Conn, t time.Time) error {
			if time.Since(t) < time.Minute {
				return nil
			}
			_, err := c.Do("PING")
			return err
		},
	}
}

func InitRedisPool(cfg string) redis.Pool {
	var redisConf RedisCfg
	redisConf.address = gjson.Get(cfg, "address").String()
	redisConf.password = gjson.Get(cfg, "password").String()
	redisConf.maxIdle = int(gjson.Get(cfg, "maxIdle").Int())
	redisConf.maxActive = int(gjson.Get(cfg, "maxActive").Int())
	redisConf.db = gjson.Get(cfg, "db").Int()
	redisConf.idleTimeout = int(gjson.Get(cfg, "idleTimeout").Int())
	log.Println("Redis connection ok")
	return newPool(redisConf)
}
