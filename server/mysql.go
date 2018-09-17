package server

import (
	"database/sql"
	"fmt"
	"sync"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/tidwall/gjson"
)

type MysqlConn struct {
	db *sql.DB
}

var mysqlDbMap map[string]*sql.DB
var mysqlConnMutex sync.Mutex

func init() {
	mysqlDbMap = make(map[string]*sql.DB)
}

func getMysqlConn(cfg string) *sql.DB {
	host := gjson.Get(cfg, "host").String()
	port := int(gjson.Get(cfg, "port").Int())
	username := gjson.Get(cfg, "username").String()
	password := gjson.Get(cfg, "password").String()
	dbname := gjson.Get(cfg, "dbname").String()
	charset := gjson.Get(cfg, "charset").String()
	maxIdle := int(gjson.Get(cfg, "maxIdle").Int())
	maxOpen := int(gjson.Get(cfg, "maxOpen").Int())
	maxLifetime := time.Duration(gjson.Get(cfg, "maxLifetime").Int())

	url := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?charset=%s", username, password, host, port, dbname, charset)

	db, _ := sql.Open("mysql", url)
	db.SetMaxOpenConns(maxOpen)
	db.SetMaxIdleConns(maxIdle)
	db.SetConnMaxLifetime(maxLifetime)
	db.Ping()

	return db
}

//获取连接
func (c *MysqlConn) GetConn() *sql.DB {
	return c.db
}

func (c *MysqlConn) Close() error {
	err := c.db.Close()
	return err
}

//插入
func (c *MysqlConn) Insert(sqlstr string, args ...interface{}) (int64, error) {
	stmtIns, err := c.db.Prepare(sqlstr)
	if err != nil {
		panic(err.Error())
	}
	defer stmtIns.Close()

	result, err := stmtIns.Exec(args...)
	if err != nil {
		panic(err.Error())
	}
	return result.LastInsertId()
}

//修改和删除
func (c *MysqlConn) Exec(sqlstr string, args ...interface{}) (int64, error) {
	stmtIns, err := c.db.Prepare(sqlstr)
	if err != nil {
		panic(err.Error())
	}
	defer stmtIns.Close()

	result, err := stmtIns.Exec(args...)
	if err != nil {
		panic(err.Error())
	}
	return result.RowsAffected()
}

//取一行数据，注意这类取出来的结果都是string
func (c *MysqlConn) FetchRow(sqlstr string, args ...interface{}) (*map[string]string, error) {
	stmtOut, err := c.db.Prepare(sqlstr)
	if err != nil {
		panic(err.Error())
	}
	defer stmtOut.Close()

	rows, err := stmtOut.Query(args...)
	if err != nil {
		panic(err.Error())
	}

	columns, err := rows.Columns()
	if err != nil {
		panic(err.Error())
	}

	values := make([]sql.RawBytes, len(columns))
	scanArgs := make([]interface{}, len(values))
	ret := make(map[string]string, len(scanArgs))

	for i := range values {
		scanArgs[i] = &values[i]
	}
	for rows.Next() {
		err = rows.Scan(scanArgs...)
		if err != nil {
			panic(err.Error())
		}
		var value string

		for i, col := range values {
			if col == nil {
				value = "NULL"
			} else {
				value = string(col)
			}
			ret[columns[i]] = value
		}
		break //get the first row only
	}
	return &ret, nil
}

//取多行
func (c *MysqlConn) FetchRows(sqlstr string, args ...interface{}) (*[]map[string]string, error) {
	stmtOut, err := c.db.Prepare(sqlstr)
	if err != nil {
		panic(err.Error())
	}
	defer stmtOut.Close()

	rows, err := stmtOut.Query(args...)
	if err != nil {
		panic(err.Error())
	}

	columns, err := rows.Columns()
	if err != nil {
		panic(err.Error())
	}

	values := make([]sql.RawBytes, len(columns))
	scanArgs := make([]interface{}, len(values))

	ret := make([]map[string]string, 0)
	for i := range values {
		scanArgs[i] = &values[i]
	}

	for rows.Next() {
		err = rows.Scan(scanArgs...)
		if err != nil {
			panic(err.Error())
		}
		var value string
		vmap := make(map[string]string, len(scanArgs))
		for i, col := range values {
			if col == nil {
				value = "NULL"
			} else {
				value = string(col)
			}
			vmap[columns[i]] = value
		}
		ret = append(ret, vmap)
	}
	return &ret, nil
}
