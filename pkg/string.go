package pkg

import (
	"strings"
)

// CamelCase2UnderScoreCase 将驼峰命名法转下划线命名。
// 如 MediaID => media_id
func CamelCase2UnderScoreCase(name string) string {
	if len(name) == 0 {
		return ""
	}
	var r = []rune(name)
	var temp = make([]rune, 0)

	for i, v := range r {
		isUpper := v >= 65 && v <= 90
		nv := v
		if isUpper {
			nv = v + 32
		}

		if isUpper && i < len(r)-1 && (r[i+1] < 65 || r[i+1] > 90) {
			// 当前大写，下一位小写
			temp = append(temp, 95) // 下划线
			temp = append(temp, nv)
		} else if isUpper && i != 0 && (r[i-1] < 65 || r[i-1] > 90) {
			// 当前大写，上一位小写
			temp = append(temp, 95) // 下划线
			temp = append(temp, nv)
		} else {
			temp = append(temp, nv)
		}
	}
	return strings.Trim(string(temp), "_")
}
