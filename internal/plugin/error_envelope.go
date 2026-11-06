package main

import (
	"encoding/json"
	"strings"
)

// upstreamBusinessError 是 CodeBuddy 上游的业务错误信封。
//
// 上游并不总是用 HTTP 状态码表达失败：限流（6004）、积分耗尽（14018）等
// 情况会以 HTTP 200 + 一个 {"code":...,"msg":"..."} 的 JSON 对象返回，
// 有时还包在 {"error":{...}} 里。这些对象不是 chat.completion.chunk，
// 协议转换器不认识它们——一旦当普通 chunk 透传给客户端，
// 客户端只会收到一个无法翻译的负载，最终表现为「空回复」。
//
// 因此凡是这种信封，都必须在插件侧识别为错误：
// 走错误帧交给宿主，让 failover 与冷却按真实原因处理。
type upstreamBusinessError struct {
	Code    int    `json:"code"`
	Message string `json:"msg"`
	// 某些 realm 用 message 而不是 msg。
	MessageAlt string `json:"message"`
	// 嵌套形式 {"error":{"code":...,"message":"..."}}。
	Error *struct {
		Code    int    `json:"code"`
		Message string `json:"message"`
	} `json:"error"`
}

// businessErrorOf 判断一段上游负载是否为业务错误信封。
// 返回非 nil 即代表这是一个错误，调用方应据此中断本次尝试。
func businessErrorOf(raw string) (*upstreamBusinessError, error) {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" || !strings.HasPrefix(trimmed, "{") {
		return nil, nil
	}
	var env upstreamBusinessError
	if json.Unmarshal([]byte(trimmed), &env) != nil {
		return nil, nil
	}
	// 平铺形式：code 非 0 即错误（code 缺省为 0 视为正常）。
	if env.Code != 0 {
		return &env, nil
	}
	// 嵌套形式：error 对象存在且带非 0 code。
	if env.Error != nil && env.Error.Code != 0 {
		env.Code = env.Error.Code
		if env.Message == "" {
			env.Message = env.Error.Message
		}
		return &env, nil
	}
	return nil, nil
}

// businessErrorAsError 把信封转成带上游原文的错误。
// 保留原始 msg，让客户端看到「usage exceeds frequency limit, reset at ...」
// 这类可操作的信息，而不是笼统的「上游错误」。
func businessErrorAsError(env *upstreamBusinessError) error {
	if env == nil {
		return nil
	}
	msg := strings.TrimSpace(env.Message)
	if msg == "" {
		msg = strings.TrimSpace(env.MessageAlt)
	}
	if msg == "" {
		msg = "upstream returned a business error envelope"
	}
	return &upstreamStatusError{status: 0, body: redactSecrets(msg), code: env.Code}
}
