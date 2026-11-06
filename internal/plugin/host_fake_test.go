// host_fake_test.go 提供测试专用的「假装宿主桥可用」开关。
//
// 生产路径要求 hostAPI 非空才走宿主分支；测试里 hostAPI 是 cgo 指针无法
// 构造，所以用 hostBridgeForced 这个纯 Go 开关放行，配合 abi.SetHostCaller
// 替换的传输来观察生产代码构造的 wire。
package main

import "testing"

// installFakeHostAPI 打开宿主分支并返回恢复函数。
func installFakeHostAPI(t *testing.T) func() {
	t.Helper()
	old := hostBridgeForced
	hostBridgeForced = true
	return func() { hostBridgeForced = old }
}
