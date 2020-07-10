package dbutil

import (
	"database/sql"
	"errors"
	"fmt"
)

// DBConn 数据查询工具类
type DBConn struct {
	DB *sql.DB
}

// Close 释放数据库连接
func (db *DBConn) Close() {
	if db.DB != nil {
		_ = db.DB.Close()
	}
}

// OpenMySQL 打开mysql连接
func (db *DBConn) OpenMySQL(host string, port int, dbName, user, pwd string) error {
	if db.DB != nil {
		return errors.New("DBConn has been opened")
	}
	addr := fmt.Sprintf("%v:%v@tcp(%v:%v)/%v", user, pwd, host, port, dbName)
	addr += "?parseTime=true&loc=Asia%2FShanghai&charset=utf8"
	// 创建连接池信息，并不会创建连接
	conn, err := sql.Open("mysql", addr)
	if err != nil {
		return err
	}
	db.DB = conn
	return nil
}

// TestConn 测试是否可以连接到数据库
func (db *DBConn) TestConn() error {
	if db.DB == nil {
		return errors.New("Dbconn has not been opened")
	}
	return db.DB.Ping()
}

// Select 查询操作，未查询到返回nil
func (db *DBConn) Select(query string, args ...interface{}) (*RowsData, error) {
	rows, err := db.DB.Query(query, args...)
	if err != nil {
		return nil, err
	}

	rsd, err := ScanRows(rows)
	if err != nil {
		return nil, err
	}

	return rsd, nil
}
