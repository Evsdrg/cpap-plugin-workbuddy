package main

import (
	"testing"

	"github.com/Evsdrg/cepvor-cpa-plinth/abi"
	"github.com/router-for-me/CLIProxyAPI/v8/sdk/pluginabi"
	"github.com/router-for-me/CLIProxyAPI/v8/sdk/pluginapi"
)

// workbuddyAuthHost 让宿主返回一份启用、一份禁用、一份其他 provider 的账号。
func workbuddyAuthHost(t *testing.T) {
	t.Helper()
	abi.SetHostCaller(func(method string, _ []byte) ([]byte, error) {
		if method != pluginabi.MethodHostAuthList {
			return abi.OK(map[string]any{})
		}
		return abi.OK(map[string]any{"files": []pluginapi.HostAuthFileEntry{
			{Name: "workbuddy-active.json", AuthIndex: "idx-1", Provider: providerName, Type: providerName, Status: "active"},
			{Name: "workbuddy-disabled.json", AuthIndex: "idx-2", Provider: providerName, Type: providerName, Status: "disabled", Disabled: true},
			{Name: "other.json", AuthIndex: "idx-3", Provider: "other", Type: "other", Status: "active"},
		}})
	})
	t.Cleanup(abi.ClearHostCaller)
}

// 调度名单不能包含已禁用账号：定时器还去叫醒一个操作员主动关掉的账号，
// 只会制造失败日志，并在面板上把「已禁用」表现成「签到失败」。
func TestAccountNamesForScheduleSkipsDisabled(t *testing.T) {
	workbuddyAuthHost(t)
	names := accountNamesForSchedule()
	if len(names) != 1 || names[0] != "workbuddy-active.json" {
		t.Fatalf("accountNamesForSchedule = %v, want only workbuddy-active.json", names)
	}
}

// hostAuthList 本身不过滤，只读查询与面板仍需要看到禁用账号。
func TestHostAuthListKeepsDisabledForDisplay(t *testing.T) {
	workbuddyAuthHost(t)
	files, err := hostAuthList()
	if err != nil {
		t.Fatalf("hostAuthList: %v", err)
	}
	if len(files) != 2 {
		t.Fatalf("hostAuthList = %d entries, want 2 workbuddy entries including the disabled one", len(files))
	}
	var seenDisabled bool
	for _, file := range files {
		if file.Disabled {
			seenDisabled = true
		}
	}
	if !seenDisabled {
		t.Fatal("the listing must keep disabled accounts visible for the panel")
	}
}

// scheduleHours 必须返回配置副本，而不是包级切片的别名。
//
// 只比对数值没有区分力：裸读和带锁读都能读到同一个当前值。真正的差别在
// 别名——直接返回 checkinHours 会把内部切片交给调度循环，循环在遍历它时
// configure() 正好整份替换该变量，两者就构成数据竞争（go test -race 可复现）。
// 这里用“改元素不影响调用方”钉住副本语义。
func TestScheduleHoursReturnsCopyNotAlias(t *testing.T) {
	t.Cleanup(func() {
		checkinHoursMu.Lock()
		checkinHours = []int{9, 21}
		checkinHoursMu.Unlock()
	})
	checkinHoursMu.Lock()
	checkinHours = []int{4, 16}
	checkinHoursMu.Unlock()

	got := scheduleHours(taskCheckin)
	if len(got) != 2 || got[0] != 4 || got[1] != 16 {
		t.Fatalf("scheduleHours(taskCheckin) = %v, want [4 16] from the configured value", got)
	}

	// 修改上一次调用返回的切片，不应影响下一次调用的结果——后者必须来自
	// 包级变量而不是共享同一个底层数组。
	got[0] = 99
	again := scheduleHours(taskCheckin)
	if again[0] != 4 {
		t.Fatalf("scheduleHours handed out an alias of the package-level slice: second call = %v", again)
	}
}
