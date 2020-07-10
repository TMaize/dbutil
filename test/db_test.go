package test

import (
	"fmt"
	"testing"

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

	rsd, err := DBconn.Select("select * from `f`")
	if err != nil {
		t.Fatal(err)
	}

	if len(rsd.Rows) == 0 {
		t.Log("未查询到数据")
		return
	}

	for rsd.Next() {
		fmt.Println(rsd.ToMap())
		fmt.Println(rsd.GetFloat32("c1"))
		fmt.Println(rsd.GetFloat32("c2"))
		fmt.Println(rsd.GetFloat32("c3"))
		fmt.Println(rsd.GetFloat32("c4"))
	}
}
