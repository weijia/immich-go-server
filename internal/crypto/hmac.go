package crypto

import (
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"strconv"
	"strings"
)

// canonical 构造参与签名的规范化串（§9.5）：
// METHOD\nPATH\nTIMESTAMP\nNONCE\nSHA256(BODY)
func canonical(method, path string, ts int64, nonce, body []byte) string {
	h := sha256.Sum256(body)
	return strings.Join([]string{
		strings.ToUpper(method),
		path,
		strconv.FormatInt(ts, 10),
		string(nonce),
		hex.EncodeToString(h[:]),
	}, "\n")
}

// SignRequest 计算集群请求 HMAC-SHA256 签名（§9.5）。
func SignRequest(secret, method, path string, ts int64, nonce, body []byte) string {
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write([]byte(canonical(method, path, ts, nonce, body)))
	return hex.EncodeToString(mac.Sum(nil))
}

// VerifyRequest 验签四道关（§9.5）：时间窗 → Nonce 防重放 → 重算并常数时间比较。
// seenNonces 由调用方持有（如 HTTP 层的内存集合），窗口内重复 Nonce 即拒绝。
func VerifyRequest(secret, method, path string, ts int64, nonce, body []byte, sig string, now, maxSkew int64, seenNonces map[string]bool) bool {
	if now < ts-maxSkew || now > ts+maxSkew {
		return false
	}
	if seenNonces[string(nonce)] {
		return false
	}
	expected := SignRequest(secret, method, path, ts, nonce, body)
	if !hmac.Equal([]byte(expected), []byte(sig)) {
		return false
	}
	seenNonces[string(nonce)] = true
	return true
}

// GenerateNonce 生成 16 字节随机 nonce。
func GenerateNonce() ([]byte, error) {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		return nil, err
	}
	return b, nil
}
