package dbutil

import (
	"database/sql"
	"errors"
	"fmt"
	"reflect"
	"strconv"
	"time"

	"github.com/TMaize/dbutil/pkg"
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
	"float": {
		"FLOAT":   true,
		"DOUBLE":  true,
		"DECIMAL": true,
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

	if goTypeMap["float"][typeName] {
		return rsd.GetFloat64(key)
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
		return strconv.ParseInt(string(data.([]byte)), 10, 64)
	case nil:
		return 0, nil
	default:
		return 0, errors.New("can not convert to " + goType)
	}
}

// GetFloat32 返回 float32
func (rsd *RowsData) GetFloat32(key string) (float32, error) {
	value, err := rsd.GetFloat64(key)
	if err != nil {
		return 0, err
	}

	f32, err := strconv.ParseFloat(fmt.Sprint(value), 32)
	if err != nil {
		return 0, err
	}

	return float32(f32), nil
}

// GetFloat64 返回 float64
func (rsd *RowsData) GetFloat64(key string) (float64, error) {
	data, err := rsd.getRaw(key)
	if err != nil {
		return 0, err
	}

	goType := "float"
	typeName := rsd.ColumnTypes[rsd.nameIndexMap[key]].DatabaseTypeName()
	if !goTypeMap[goType][typeName] {
		return 0, fmt.Errorf("unsupported conversion: db(%s) => go(%s)", typeName, goType)
	}

	switch data.(type) {
	case float32, float64:
		return data.(float64), nil
	case []byte: // []uint8
		// ASCII byte 可以直接转强为string
		return strconv.ParseFloat(string(data.([]byte)), 64)
	case nil:
		return 0, nil
	default:
		return 0, errors.New("can not convert to " + goType)
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
		return time.Time{}, errors.New("can not convert to " + goType)
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
		return "", errors.New("can not convert to " + goType)
	}
}

// GetStruct 填充 struct
func (rsd *RowsData) GetStruct(model interface{}) error {
	// nil无需scan
	if model == nil {
		return nil
	}

	value := reflect.ValueOf(model)

	if value.Kind() != reflect.Ptr {
		return errors.New("model must be pointer, type is " + value.Kind().String())
	}

	// 检查指针对应的值是否为nil
	if value.Kind() == reflect.Ptr && value.IsNil() {
		return nil
	}

	if reflect.Indirect(value).Kind() != reflect.Struct {
		return errors.New("model type must be struct, type is " + reflect.Indirect(value).Kind().String())
	}

	// Indirect获取指针对应的数据的类型,用于反射出属性
	structInfo := reflect.TypeOf(reflect.Indirect(value).Interface())

	// Elem 指针修改数据的引用
	elem := value.Elem()

	for i := 0; i < structInfo.NumField(); i++ {
		// 根据struct的属性生成3个可能的值去重table列中去取
		keys := []string{
			structInfo.Field(i).Tag.Get("column"),
			structInfo.Field(i).Name,
			pkg.CamelCase2UnderScoreCase(structInfo.Field(i).Name),
		}

		var key string

		for _, temp := range keys {
			if _, ok := rsd.nameIndexMap[temp]; ok {
				key = temp
				break
			}
		}

		if key == "" {
			// fmt.Printf("search %v skip for struct(%s) skiped\n", keys, structInfo.Field(i).Name)
			continue
		}

		// fmt.Printf("struct(%s) type is %s\n", structInfo.Field(i).Name, structInfo.Field(i).Type.String())

		switch elem.Field(i).Interface().(type) {
		case int:
			intValue, err := rsd.GetInt(key)
			if err != nil {
				return err
			}
			elem.Field(i).Set(reflect.ValueOf(intValue))
		case int64:
			int64Value, err := rsd.GetInt64(key)
			if err != nil {
				return err
			}
			elem.Field(i).Set(reflect.ValueOf(int64Value))
		case float32:
			f32Value, err := rsd.GetFloat32(key)
			if err != nil {
				return err
			}
			elem.Field(i).Set(reflect.ValueOf(f32Value))
		case float64:
			f64Value, err := rsd.GetFloat64(key)
			if err != nil {
				return err
			}
			elem.Field(i).Set(reflect.ValueOf(f64Value))
		case string:
			sValue, err := rsd.GetString(key)
			if err != nil {
				return err
			}
			elem.Field(i).Set(reflect.ValueOf(sValue))
		case time.Time:
			timeValue, err := rsd.GetTime(key)
			if err != nil {
				return err
			}
			elem.Field(i).Set(reflect.ValueOf(timeValue))
		}
	}
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
