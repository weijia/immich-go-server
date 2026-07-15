// Package eval 提供目录温度与访问分的启发式评估（§温度/访问代理）。
//
// 项目当前尚未接入真实访问埋点，故以 dirKey（形如 "YYYY/MM"）推断的摄入年月
// 作为"年龄"：年龄越大越冷（24 个月线性冷却至 0）。accessScore 由温度与体量
// 因子合成。一旦接入真实访问日志，替换 Evaluate 的数据源即可，调用方无需改动。
package eval

import (
	"strconv"
	"strings"
	"time"
)

// Evaluate 返回目录的温度(0~1)与访问分(0~1)。
//   - dirKey:     目录键，优先解析 "YYYY/MM" 作为摄入年月
//   - totalBytes: 该目录物理总字节
//   - now:        评估时刻（epoch 秒，当前仅用于占位/未来扩展）
func Evaluate(dirKey string, totalBytes, now int64) (temperature, accessScore float64) {
	ageMonths := ageInMonths(dirKey)
	t := 1.0 - float64(ageMonths)/24.0
	if t < 0 {
		t = 0
	}
	if t > 1 {
		t = 1
	}
	// 体量因子：越大视为越重要/越常访问（粗糙代理），以 1GiB 饱和。
	sizeFactor := float64(totalBytes) / float64(totalBytes+(1<<30))
	if sizeFactor < 0.1 {
		sizeFactor = 0.1
	}
	if sizeFactor > 1 {
		sizeFactor = 1
	}
	acc := t * sizeFactor
	if acc < 0 {
		acc = 0
	}
	if acc > 1 {
		acc = 1
	}
	return t, acc
}

// ageInMonths 由 dirKey 的 "YYYY/MM" 推算相对当前月份的月龄（>=0）。
func ageInMonths(dirKey string) int {
	parts := strings.Split(dirKey, "/")
	if len(parts) < 2 {
		return 0
	}
	y, err1 := strconv.Atoi(parts[0])
	m, err2 := strconv.Atoi(parts[1])
	if err1 != nil || err2 != nil || m < 1 || m > 12 {
		return 0
	}
	ny, nm, _ := time.Now().Date()
	cur := ny*12 + int(nm)
	got := y*12 + m
	a := cur - got
	if a < 0 {
		a = 0
	}
	return a
}
