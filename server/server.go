package server

import (
	"log"
	"recommend/util"

	"github.com/tidwall/gjson"
)

type Config struct {
	Json string
}

var err error

func (t *Config) ReadJsonFile(file string) string {
	t.Json, err = util.ReadFile(file)
	if err != nil {
		log.Fatalln(err)
	}
	return t.Json
}

func (t *Config) Get(key string) gjson.Result {
	return gjson.Get(t.Json, key)
}
