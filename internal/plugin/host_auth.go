// host_auth.go 封装宿主的 auth 存储 RPC（host.auth.list / get / get_bundle）。
//
// 这是插件读取凭据文件的唯一入口；写入走 authfile.go 的 hostAuthPersist /
// hostAuthPersistMigrate。
//
// 这里不再手工解 envelope：SDK 的 abi.HostCallInto 已经做了「调用 → 校验
// ok → 解 result」这三步，重复一遍只会多一处可能和宿主协议漂移的代码。
package main

import (
	"strings"

	"github.com/Evsdrg/cepvor-cpa-plinth/abi"
	"github.com/router-for-me/CLIProxyAPI/v8/sdk/pluginabi"
	"github.com/router-for-me/CLIProxyAPI/v8/sdk/pluginapi"
)

// hostAuthList 返回宿主已知的全部 workbuddy 凭据。
//
// 按文件名前缀过滤，而不是按 Type/Provider：磁盘上不少既有 auth 文件没有
// type/provider 字段（它们是那条约定之前写下的），而 EqualFold("", providerName)
// 对这些文件返回 false，会把本该保留的 workbuddy- 文件错误排除。文件名前缀
// 是唯一可靠的跨版本判据。
func hostAuthList() ([]pluginapi.HostAuthFileEntry, error) {
	var resp struct {
		Files []pluginapi.HostAuthFileEntry `json:"files"`
	}
	if err := abi.HostCallInto(pluginabi.MethodHostAuthList, nil, &resp); err != nil {
		return nil, err
	}
	// 新切片而不是 resp.Files[:0]：后者会复用 RPC 响应的底层数组，若 resp
	// 将来被缓存或复用就会串数据。
	out := make([]pluginapi.HostAuthFileEntry, 0, len(resp.Files))
	prefix := providerName + "-"
	for _, file := range resp.Files {
		if strings.HasPrefix(strings.ToLower(file.Name), prefix) {
			out = append(out, file)
		}
	}
	return out, nil
}

// hostAuthGet 按 auth index 取回并解析凭据。
func hostAuthGet(authIndex string) (*storedAuth, error) {
	phys, err := hostAuthGetPhysical(authIndex)
	if err != nil {
		return nil, err
	}
	return parseStored(phys.JSON)
}

// hostAuthGetBundle 一次 host.auth.get 同时拿到解析后的凭据与物理元数据
// （避免面板原先的 get + getPhysical 两次 RPC）。
func hostAuthGetBundle(authIndex string) (*storedAuth, *hostAuthPhysical, error) {
	phys, err := hostAuthGetPhysical(authIndex)
	if err != nil {
		return nil, nil, err
	}
	sa, err := parseStored(phys.JSON)
	if err != nil {
		return nil, phys, err
	}
	return sa, phys, nil
}
