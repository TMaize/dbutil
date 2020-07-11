package test

import (
	"fmt"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"

	"github.com/TMaize/dbutil"
)

var DBconn *dbutil.DBConn

const DBHost = "127.0.0.1"
const DBPort = 3306
const DBName = "test"
const DBUser = "root"
const DBPassword = "123456789"

func TestMain(m *testing.M) {
	m.Run()
	if DBconn != nil {
		fmt.Println("释放连接")
		DBconn.Close()
	}
}

func TestDBConn(t *testing.T) {
	if DBconn != nil {
		return
	}
	dbc := dbutil.DBConn{}
	err := dbc.OpenMySQL(DBHost, DBPort, DBName, DBUser, DBPassword)
	if err != nil {
		t.Fatal(err)
	}

	err = dbc.TestConn()
	if err != nil {
		t.Fatal(err)
	}
	DBconn = &dbc
}

func TestDBSelect(t *testing.T) {
	t.Run("初始化连接", TestDBConn)

	rsd, err := DBconn.Select("select * from `demo`")
	if err != nil {
		t.Fatal(err)
	}

	if len(rsd.Rows) == 0 {
		t.Log("未查询到数据")
		return
	}
	type User struct {
		ID         int
		Name       string `column:"user_name"`
		Age        int    `column:"user_age"`
		CreateTime time.Time
	}

	for rsd.Next() {
		u := User{}
		fmt.Println(rsd.GetStruct(&u))
		fmt.Printf("%+v\n", u)
	}
}
