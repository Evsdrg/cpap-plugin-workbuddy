package main

import (
	"os"
	"strings"
	"testing"
)

// VERSION 文件与源码里的 version 变量必须一致。
//
// 这条测试是补出来的：0.14.0 部署时 VERSION 改成了 0.14.0，但 main.go 里
// `var version = "0.13.0"` 是硬编码、构建时没有 -ldflags 注入，于是宿主日志
// 里出现「version=0.13.0 path=...workbuddy-v0.14.0.so」这种自相矛盾的记录。
// 版本声明是宿主判断插件身份与 prunes 旧版本的依据，不能靠人工同步。
func TestVersionMatchesVersionFile(t *testing.T) {
	raw, err := os.ReadFile("VERSION")
	if err != nil {
		t.Fatalf("read VERSION: %v", err)
	}
	want := strings.TrimSpace(string(raw))
	if want == "" {
		t.Fatal("VERSION file is empty")
	}
	if version != want {
		t.Fatalf("main.version = %q but VERSION = %q; update the var in main.go (build does not inject -ldflags)", version, want)
	}
}
