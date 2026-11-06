package main

import (
	"errors"
	"strings"
	"time"

	"github.com/Evsdrg/cepvor-cpa-plinth/abi"
	"github.com/Evsdrg/cepvor-cpa-plinth/catalog"
	"github.com/router-for-me/CLIProxyAPI/v8/sdk/pluginapi"
)

// catalogueScheduler 驱动后台模型目录刷新。间隔为 0 时保持停止——一次刷新要按
// 账号各走一次上游往返，必须由配置显式开启。
//
// 用 SDK 的调度器而不是自建 ticker，是因为那一份把「刷新失败」「目录未变」
// 「发布失败」三件事分开记录，管理页能据此区分「按钮有响应」和「目录真的变了」。
//
// 这是对宿主 ABI 缺 `model.refresh` 方法的绕行：插件无法直接叫宿主重读目录，
// 只能先改自己的缓存，再通过触发一次凭据写入让宿主重走 model.for_auth。
// 宿主补上 model.refresh 之后，这里应改成直接调用它。
var catalogueScheduler = catalog.NewScheduler(0)

// configureCatalogueRefresh 按配置武装或拆除刷新循环。
func configureCatalogueRefresh(interval time.Duration) {
	catalogueScheduler.SetInterval(interval)
	if !catalogueScheduler.Enabled() {
		catalogueScheduler.Stop()
		return
	}
	// 一次 tick 不属于任何一次客户端请求，因此用空 Host：插件装载期间这条
	// 传输通道一直在，宿主会把日志归到插件而不是某次调用。
	host := &abi.Host{}
	catalogueScheduler.Start(catalog.Request{
		Host:     host,
		Provider: providerName,
		// 只有目录真的变了才回写凭据文件。目录稳定时不产生任何写，避免与
		// 宿主的 token 续期抢同一个文件。
		PublishOnChange: true,
		Refresh:         func() (catalog.Outcome, error) { return refreshWorkbuddyCatalogue(host) },
	})
}

// stopCatalogueRefresh 在卸载时结束后台循环。
func stopCatalogueRefresh() { catalogueScheduler.Stop() }

// catalogueStatus 返回循环状态，供管理接口展示。
func catalogueStatus() (interval time.Duration, runs int, lastRun time.Time, lastErr string) {
	return catalogueScheduler.Status()
}

// refreshWorkbuddyCatalogue 重拉一个有可用凭据的账号的目录并写入缓存。
//
// Changed 必须与刷新前的快照比较：只报数量无法说明目录是否真的变了，而自动
// 发布正是以 Changed 为触发条件。比较用 id 集合，因为只有模型 id 影响注册。
func refreshWorkbuddyCatalogue(host *abi.Host) (catalog.Outcome, error) {
	previous := dynamicModelIDs()

	// 先清掉缓存，让下面的取目录走真实上游而不是命中旧值。
	invalidateDynamicModelsCache()

	storage, errStorage := catalogueRefreshStorage()
	if errStorage != nil {
		return catalog.Outcome{}, errStorage
	}
	models := fetchDynamicModelsFromStorage(storage)
	if len(models) == 0 {
		return catalog.Outcome{}, errNoCatalogueModels
	}
	return catalog.Outcome{
		Models:  len(models),
		Changed: !sameModelIDSet(previous, models),
	}, nil
}

// dynamicModelIDs 取当前缓存的模型 id 集合；没有缓存时返回 nil。
//
// 缓存按「realm + 账号等级」分桶，所以这里要合并所有桶：刷新只为判断
// 「目录是否变化」，任何桶变了都算变了。
func dynamicModelIDs() []string {
	dynamicModelsCache.RLock()
	defer dynamicModelsCache.RUnlock()
	if len(dynamicModelsCache.entries) == 0 {
		return nil
	}
	seen := make(map[string]struct{})
	for _, entry := range dynamicModelsCache.entries {
		for _, model := range entry.models {
			seen[model.ID] = struct{}{}
		}
	}
	ids := make([]string, 0, len(seen))
	for id := range seen {
		ids = append(ids, id)
	}
	return ids
}

// invalidateDynamicModelsCache 丢弃缓存，让下一次取目录走真实上游。
func invalidateDynamicModelsCache() {
	dynamicModelsCache.Lock()
	dynamicModelsCache.entries = nil
	dynamicModelsCache.Unlock()
}

// sameModelIDSet 比较两份模型清单的 id 集合是否一致（不看顺序与元数据）。
func sameModelIDSet(previous []string, current []pluginapi.ModelInfo) bool {
	if len(previous) != len(current) {
		return false
	}
	seen := make(map[string]bool, len(previous))
	for _, id := range previous {
		seen[id] = true
	}
	for _, model := range current {
		if !seen[model.ID] {
			return false
		}
		delete(seen, model.ID)
	}
	return len(seen) == 0
}

// errNoCatalogueModels 表示刷新拿到了空目录。
// 不能把它当成「目录变成一个都没有」去发布：空结果更可能是上游瞬时故障，
// 据此重新注册会把该 provider 的模型全部隐藏。
var errNoCatalogueModels = errors.New("模型目录为空，保留原目录")

// catalogueRefreshStorage 取一个可用于拉目录的凭据。
//
// 刷新不属于任何一次请求，所以从宿主列出第一个启用且有凭据的账号。
// 目录本身按账号 realm 区分（CN 与国际版不同），取第一个可用账号是保守选项——
// 各 realm 的差异由请求时按凭据分派处理，这里只为推进刷新循环。
func catalogueRefreshStorage() ([]byte, error) {
	files, errList := loadAuthList()
	if errList != nil {
		return nil, errList
	}
	for _, file := range files {
		if listAuthEntryDisabled(file) || strings.TrimSpace(file.AuthIndex) == "" {
			continue
		}
		physical, errGet := hostAuthGetPhysical(file.AuthIndex)
		if errGet != nil || len(physical.JSON) == 0 {
			continue
		}
		if _, ok := extractAccessToken(physical.JSON); ok {
			return physical.JSON, nil
		}
	}
	return nil, errors.New("没有可用于拉取模型目录的账号")
}
