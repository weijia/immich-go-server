package model

import (
	"os"
	"path/filepath"
	"strings"
	"unicode"
)

const (
	sha256HexLen  = 64  // sha256 hex 长度（内容寻址的稳定前缀，始终是文件名开头）
	compBudget    = 200 // 单文件名组件字节上限（远低于 NTFS/ext4/APFS 的 255），保证跨 FS 安全
	fullPathBudget = 250 // Windows MAX_PATH(260) 留余量；全路径（baseDir+组件）不超过此值
	nameRuneBudget = 120 // 名字片段按 rune 截断上限，避免多字节 UTF-8 被切断
)

// sanitizeName 清洗原始文件名为安全的文件名片段：
//   - 移除 Windows/Unix 非法字符 < > : " / \ | ? * 与控制字符
//   - 折叠多余空白
//   - 先按 rune 截断到 nameRuneBudget，再按字节上限 byteBudget 收敛（避免 UTF-8 越界）
func sanitizeName(s string, byteBudget int) string {
	var b strings.Builder
	for _, r := range s {
		switch r {
		case '<', '>', ':', '"', '/', '\\', '|', '?', '*':
			continue
		}
		if unicode.IsControl(r) {
			continue
		}
		b.WriteRune(r)
	}
	clean := strings.Join(strings.Fields(b.String()), " ") // 折叠空白并掐头尾
	runes := []rune(clean)
	if len(runes) > nameRuneBudget {
		runes = runes[:nameRuneBudget]
	}
	for len(string(runes)) > byteBudget && len(runes) > 0 {
		runes = runes[:len(runes)-1]
	}
	return strings.TrimSpace(string(runes))
}

// physName 内部实现：<namePart>[_<assetID>][.<ext>]（原始名在前、sha256 在后，便于按原名浏览），
// namePart 字节上限由 nameByteBudget 约束。assetID 始终作为唯一后缀保身份/校验/去重。
func physName(assetID, originalPath string, nameByteBudget int) string {
	ext := strings.TrimSpace(strings.TrimPrefix(filepath.Ext(originalPath), "."))
	ext = strings.ToLower(ext)
	base := filepath.Base(originalPath)
	namePart := sanitizeName(strings.TrimSuffix(base, filepath.Ext(base)), nameByteBudget)
	switch {
	case namePart == "" && ext == "":
		return assetID
	case namePart == "":
		return assetID + "." + ext
	case ext == "":
		return namePart + "_" + assetID
	default:
		return namePart + "_" + assetID + "." + ext
	}
}

// PhysName 返回内容寻址的物理文件名（固定组件字节预算，跨目录安全）。
// assetID = sha256（去重/校验/幂等的基础）；namePart = 原始文件名清洗/截断片段（便于人识别，纯信息性）。
func PhysName(assetID, originalPath string) string {
	return physName(assetID, originalPath, compBudget)
}

// PhysNameInDir 已知写入目录时，按全路径长度动态收窄名字片段预算，进一步防止超过文件系统路径限制。
func PhysNameInDir(assetID, originalPath, baseDir string) string {
	budget := fullPathBudget - len(baseDir) - 1 - sha256HexLen - 1 // -1 分隔符, -1 安全余量
	budget = min(budget, compBudget)
	if budget < 8 { // 极端：baseDir 过长，保障至少保留 sha256 前缀
		budget = 8
	}
	return physName(assetID, originalPath, budget)
}

// ResolvePhysPath 在 baseDir 下定位 assetID 的物理文件，兼容多种形态：
//  1. originalPath 能提供 ext/名字则优先 PhysName 精确匹配（Stat 校验存在）；
//  2. 否则在 baseDir 中按 sha256 后缀 glob *<assetID>*（覆盖 <name>_<assetID>.<ext>、<assetID>_<name>.<ext>、裸 <assetID> 等历史形态），取首个非 sidecar 匹配；
//  3. 再回退裸 <assetID>。
//
// 用于读取/删除/去重等只需定位、不一定持有 originalPath 的场景（集群间拉取、迁移源、扫描存量）。
func ResolvePhysPath(baseDir, assetID, originalPath string) string {
	if cand := physName(assetID, originalPath, compBudget); cand != "" {
		if p := filepath.Join(baseDir, cand); statOK(p) {
			return p
		}
	}
	if matches, err := filepath.Glob(filepath.Join(baseDir, "*"+assetID+"*")); err == nil {
		for _, m := range matches {
			if !strings.HasSuffix(m, ".meta.json") {
				return m
			}
		}
	}
	return filepath.Join(baseDir, assetID)
}

// FirstMatch 返回 baseDir 下含 assetID 的首个非 sidecar 匹配（按后缀定位，迁移源定位用）。
func FirstMatch(baseDir, assetID string) (string, bool) {
	matches, err := filepath.Glob(filepath.Join(baseDir, "*"+assetID+"*"))
	if err != nil || len(matches) == 0 {
		return "", false
	}
	for _, m := range matches {
		if !strings.HasSuffix(m, ".meta.json") {
			return m, true
		}
	}
	return "", false
}

func statOK(p string) bool {
	_, err := os.Stat(p)
	return err == nil
}
