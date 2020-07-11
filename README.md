# dbutil

简单的 sql 查询工具方法，方便做一些简单的查询映射到 Go 类型。如果受不了原生的 scan 和 不习惯 orm 框架的可以试试这个

```bash
go get -u github.com/TMaize/dbutil
```

## 工具类

DBConn

```
OpenMySQL() // 打开mysql连接
Close()     // 关闭连接
TestConn()  // 测试连接
Select()    // 查询sql，返回 RowsData
```

RowsData

通过`DBConn.select`返回或手动调用`ScanRows`初始化

```
ColumnNames   // 所有列名
Rows          // 所有列数据

ScanRows()    // 手动使用sql.Rows初始化
NameAt()      // 某一列的列名
Next()        // 移动到下一行
Seek()        // 移动到某一行

ToMap()       // 当前行转换为 map[string]interface{}

Get()         // 返回合适的类型到interface{}
GetInt()      // int
GetInt64()    // int64
GetFloat32()  // float32
GetFloat64()  // float64
GetTime()     // time.Time
GetString()   // string
GetStruct()   // 当前行结果映射到struct
```

## GetStruct 说明

映射对象必须是 struct,传入的必须是指针类型，在映射时，会依次从：tag 中定义的 column，属性名，属性名转小写下划线命名，这三种 key 去 table 列中取值，如果未命中则该属性不会被赋值

```go
type User struct {
  ID         int       // [ID, id]
  Name       string    `column:"user_name"` //[user_name, Name, name]
  Age        int       `column:"user_age"`  //[user_age, Age, age]
  CreateTime time.Time // [CreateTime, create_time]
}
```
