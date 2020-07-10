package dbutil

import (
	"database/sql"
	"errors"
	"fmt"
	"strconv"
	"time"
)

// GO 类型对应那些 DB 类型
var goTypeMap = map[string]map[string]bool{
	"int": {
		"INT":       true,
		"TINYINT":   true,
		"SMALLINT":  true,
		"MEDIUMINT": true,
		"BIGINT":    true,
		"YEAR":      true,
	},
	"string": {
		"CHAR":       true,
		"VARCHAR":    true,
		"TINYTEXT":   true,
		"TEXT":       true,
		"MEDIUMTEXT": true,
		"LONGTEXT":   true,
		"JSON":       true,
		"TIME":       true, // hh:mm:ss
	},
	"Time": {
		"DATE":      true,
		"DATETIME":  true,
		"TIMESTAMP": true,
		"YEAR":      true, // int    转换为 yyyy-01-01 00:00:00
		"TIME":      true, // string 转换为 0001-01-01 hh:mm:ss
	},
}

// RowsData sql.Rows结果集封装
type RowsData struct {
	currentRow   int
	nameIndexMap map[string]int
	ColumnTypes  []sql.ColumnType
	ColumnNames  []string
	Rows         [][]interface{}
}

// ScanRows 转化为 RowsData
func ScanRows(rows *sql.Rows) (*RowsData, error) {
	defer rows.Close()

	columns, err := rows.ColumnTypes()
	if err != nil {
		return nil, err
	}

	rsd := &RowsData{
		ColumnTypes:  make([]sql.ColumnType, 0),
		ColumnNames:  make([]string, 0),
		Rows:         make([][]interface{}, 0),
		nameIndexMap: make(map[string]int),
		currentRow:   -1,
	}

	// 存储列信息
	for i, ct := range columns {
		key := ct.Name()
		rsd.nameIndexMap[key] = i
		rsd.ColumnNames = append(rsd.ColumnNames, key)
		rsd.ColumnTypes = append(rsd.ColumnTypes, *ct)
	}

	for rows.Next() {
		cols := make([]interface{}, len(columns))

		// 填充为指针类型
		for i := range cols {
			cols[i] = new(interface{})
		}

		err = rows.Scan(cols...)
		if err != nil {
			return nil, err
		}

		// 转回指针，然后取出指针对应的数据
		for i := range cols {
			cols[i] = *(cols[i].(*interface{}))
		}

		rsd.Rows = append(rsd.Rows, cols)
	}

	return rsd, err
}

// Next 移动到下一行
func (rsd *RowsData) Next() bool {
	n := rsd.currentRow + 1
	if n < len(rsd.Rows) && n >= 0 {
		rsd.currentRow = n
		return true
	}
	return false
}

// Seek 移动到某一行
func (rsd *RowsData) Seek(r int) bool {
	if r < len(rsd.Rows) && r >= 0 {
		rsd.currentRow = r
		return true
	}
	return false
}

// NameAt 第n列的列名
func (rsd *RowsData) NameAt(n int) string {
	if n < len(rsd.ColumnNames) && n >= 0 {
		return rsd.ColumnNames[n]
	}
	return ""
}

// getRaw 获取原始类型
func (rsd *RowsData) getRaw(key string) (interface{}, error) {
	i, ok := rsd.nameIndexMap[key]
	if !ok {
		return nil, fmt.Errorf(`no column named "%s"`, key)
	}
	return rsd.Rows[rsd.currentRow][i], nil
}

// Get 返回interface{}
func (rsd *RowsData) Get(key string) (interface{}, error) {
	typeName := rsd.ColumnTypes[rsd.nameIndexMap[key]].DatabaseTypeName()

	if goTypeMap["int"][typeName] {
		return rsd.GetInt64(key)
	}

	if goTypeMap["string"][typeName] {
		return rsd.GetString(key)
	}

	if goTypeMap["Time"][typeName] {
		return rsd.GetTime(key)
	}

	return rsd.getRaw(key)
}

// GetInt 返回int64
func (rsd *RowsData) GetInt(key string) (int, error) {
	v64, err := rsd.GetInt64(key)
	if err != nil {
		return 0, err
	}
	s64 := strconv.FormatInt(v64, 10)
	return strconv.Atoi(s64)
}

// GetInt64 返回int64
func (rsd *RowsData) GetInt64(key string) (int64, error) {
	data, err := rsd.getRaw(key)
	if err != nil {
		return 0, err
	}

	goType := "int"
	typeName := rsd.ColumnTypes[rsd.nameIndexMap[key]].DatabaseTypeName()
	if !goTypeMap[goType][typeName] {
		return 0, fmt.Errorf("unsupported conversion: db(%s) => go(%s)", typeName, goType)
	}

	switch data.(type) {
	case int, int64, int32, int16, int8:
		return data.(int64), nil
	case []byte: // []uint8
		// ASCII byte 可以直接转强为string
		s := string(data.([]byte))
		return strconv.ParseInt(s, 10, 64)
	case nil:
		return 0, nil
	default:
		return 0, errors.New("can not convert to int")
	}
}

// GetTime 返回time.Time
func (rsd *RowsData) GetTime(key string) (time.Time, error) {
	data, err := rsd.getRaw(key)
	if err != nil {
		return time.Time{}, err
	}
	goType := "Time"
	typeName := rsd.ColumnTypes[rsd.nameIndexMap[key]].DatabaseTypeName()
	if !goTypeMap[goType][typeName] {
		return time.Time{}, fmt.Errorf("unsupported conversion: db(%s) => go(%s)", typeName, goType)
	}

	switch data.(type) {
	case time.Time:
		return data.(time.Time), nil
	case nil:
		return time.Time{}, nil
	case []byte: // []uint8
		t := &sql.NullTime{}
		t.Scan(data)
		if !t.Time.IsZero() {
			return t.Time, nil
		}
		if "TIME" == typeName {
			return time.Parse("2006-01-02 15:04:05", "0001-01-01 "+string(data.([]byte)))
		}
		if "YEAR" == typeName {
			return time.Parse("2006-01-02 15:04:05", string(data.([]byte))+"-01-01 00:00:00")
		}
		return time.Time{}, nil
	default:
		return time.Time{}, errors.New("can not convert to time.Time")
	}
}

// GetString 返回string
func (rsd *RowsData) GetString(key string) (string, error) {
	data, err := rsd.getRaw(key)
	if err != nil {
		return "", err
	}
	goType := "string"
	typeName := rsd.ColumnTypes[rsd.nameIndexMap[key]].DatabaseTypeName()
	if !goTypeMap[goType][typeName] {
		return "", fmt.Errorf("unsupported conversion: db(%s) => go(%s)", typeName, goType)
	}

	switch data.(type) {
	case string:
		return data.(string), nil
	case []byte: // []uint8
		return string(data.([]byte)), nil
	case nil:
		return "", nil
	default:
		return "", errors.New("can not convert to string")
	}
}

// GetStruct 填充 struct
func (rsd *RowsData) GetStruct(s interface{}) error {
	// TODO 反射
	return nil
}

// ToMap 返回map[string]interface{}
func (rsd *RowsData) ToMap() (map[string]interface{}, error) {
	rMap := make(map[string]interface{})
	for _, key := range rsd.ColumnNames {
		v, err := rsd.Get(key)
		if err != nil {
			return rMap, err
		}
		rMap[key] = v
	}
	return rMap, nil
}


