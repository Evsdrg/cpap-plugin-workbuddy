package main

import (
	"encoding/json"
	"errors"
	"net/http"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/Evsdrg/cepvor-cpa-plinth/abi"
	"github.com/router-for-me/CLIProxyAPI/v8/sdk/pluginapi"
)

// 凭据自己的到期时间是唯一的判断依据。两种外层形状都必须能读到：parseStored
// 认扁平与嵌套两种，读到期时间若只认一种，另一种会被当成「永不到期」而永不续期。
func TestWorkbuddyCredentialExpiryReadsBothShapes(t *testing.T) {
	future := time.Now().Add(time.Hour).Unix()
	cases := []struct {
		name string
		raw  string
	}{
		{"nested", `{"auth":{"accessToken":"a","refreshToken":"r","expiresAt":` + itoa(future) + `}}`},
		{"flat", `{"accessToken":"a","refreshToken":"r","expiresAt":` + itoa(future) + `}`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			expiry, ok := workbuddyCredentialExpiry(json.RawMessage(tc.raw))
			if !ok {
				t.Fatalf("expiry not recognized for %s", tc.name)
			}
			if expiry.Unix() != future {
				t.Fatalf("expiry = %d, want %d", expiry.Unix(), future)
			}
		})
	}
}

// 没有 expiresAt 的凭据不能被编造一个到期时间：那会让 refresher 对它无限续期。
func TestWorkbuddyCredentialExpiryRejectsMissingExpiry(t *testing.T) {
	for _, raw := range []string{
		`{"auth":{"accessToken":"a"}}`,
		`{"accessToken":"a"}`,
		`{"auth":{"expiresAt":0}}`,
		`{"expiresAt":-1}`,
		`not json`,
	} {
		if expiry, ok := workbuddyCredentialExpiry(json.RawMessage(raw)); ok || !expiry.IsZero() {
			t.Fatalf("expiry(%s) = %v,%v; want zero,false", raw, expiry, ok)
		}
	}
}

func TestNextRefreshAfterUsesTenMinuteLead(t *testing.T) {
	expiry := time.Now().Add(time.Hour).Unix()
	got := nextRefreshAfter(expiry)
	want := time.Unix(expiry, 0).Add(-refreshLead)
	if !got.Equal(want) {
		t.Fatalf("nextRefreshAfter = %v, want %v", got, want)
	}
	if !nextRefreshAfter(0).IsZero() {
		t.Fatal("missing expiry must not invent a host refresh time")
	}
}

// 会话被上游注销时 refresh token 已死:必须返回终态，否则 refresher 会按普通
// 失败反复重试同一个死凭据。
func TestRefreshCredentialFlagsDeadSession(t *testing.T) {
	restore := stubRefreshCall(func(*storedAuth) (json.RawMessage, []byte, int, error) {
		return nil, []byte(`{"code":12153,"msg":"session invalid"}`), http.StatusUnauthorized,
			errors.New("upstream 401")
	})
	defer restore()

	_, err := refreshWorkbuddyCredential(&abi.Host{}, pluginapiAuthRefresh(t,
		`{"auth":{"accessToken":"old","refreshToken":"dead"}}`))
	if err == nil {
		t.Fatal("expected an error for a dead session")
	}
	var typed *abi.EnvelopeError
	if !errors.As(err, &typed) || typed.HTTPStatus != http.StatusUnauthorized {
		t.Fatalf("error = %v, want a 401 envelope error", err)
	}
	if typed.Code != "session_dead" {
		t.Fatalf("code = %q, want session_dead", typed.Code)
	}
}

// 普通 401（不是会话注销）不能标成 session_dead：那会把一次可恢复的拒绝
// 说成必须重新登录。
func TestRefreshCredentialDistinguishesOrdinaryRejection(t *testing.T) {
	restore := stubRefreshCall(func(*storedAuth) (json.RawMessage, []byte, int, error) {
		return nil, []byte(`{"code":500,"msg":"nope"}`), http.StatusUnauthorized, errors.New("upstream 401")
	})
	defer restore()

	_, err := refreshWorkbuddyCredential(&abi.Host{}, pluginapiAuthRefresh(t,
		`{"auth":{"accessToken":"old","refreshToken":"r"}}`))
	var typed *abi.EnvelopeError
	if !errors.As(err, &typed) || typed.Code != "refresh_rejected" {
		t.Fatalf("error = %v, want refresh_rejected", err)
	}
}

// 没有 refreshToken 的凭据不该去打扰上游。
func TestRefreshCredentialSkipsWhenNotRefreshable(t *testing.T) {
	called := false
	restore := stubRefreshCall(func(*storedAuth) (json.RawMessage, []byte, int, error) {
		called = true
		return nil, nil, 200, nil
	})
	defer restore()

	_, err := refreshWorkbuddyCredential(&abi.Host{}, pluginapiAuthRefresh(t,
		`{"auth":{"accessToken":"old"}}`))
	if err == nil || !strings.Contains(err.Error(), "refreshToken") {
		t.Fatalf("error = %v, want a refreshToken complaint", err)
	}
	if called {
		t.Fatal("the vendor must not be called when there is no refresh token")
	}
}

// 刚签发的凭据不该带 FileName：让 SDK 用宿主已知的名字写回，否则同一个账号
// 会长出第二份凭据文件。
func TestRefreshCredentialLeavesFileNameToTheHost(t *testing.T) {
	restore := stubRefreshCall(func(*storedAuth) (json.RawMessage, []byte, int, error) {
		return json.RawMessage(`{"accessToken":"new","refreshToken":"new-r","expiresIn":3600}`),
			nil, http.StatusOK, nil
	})
	defer restore()

	response, err := refreshWorkbuddyCredential(&abi.Host{}, pluginapiAuthRefresh(t,
		`{"auth":{"accessToken":"old","refreshToken":"r"},"account":{"uid":"uid-1"}}`))
	if err != nil {
		t.Fatalf("refreshWorkbuddyCredential: %v", err)
	}
	if response.Auth.FileName != "" || response.Auth.ID != "" {
		t.Fatalf("FileName/ID = %q/%q, want empty so the host keeps its own name", response.Auth.FileName, response.Auth.ID)
	}
	if !strings.Contains(string(response.Auth.StorageJSON), "new") {
		t.Fatalf("storage does not carry the new token: %s", response.Auth.StorageJSON)
	}
	if response.NextRefreshAfter.IsZero() {
		t.Fatal("NextRefreshAfter must be set when the vendor returned expiresIn")
	}
}

// 续期响应缺 expiresIn 时必须沿用它自己的旧到期时间，而不是把 0 写进去——
// 0 会让宿主认为凭据永久过期，触发每个请求都续期。
func TestRefreshCredentialPreservesExpiryWhenResponseOmitsIt(t *testing.T) {
	old := time.Now().Add(20 * time.Minute).Unix()
	restore := stubRefreshCall(func(*storedAuth) (json.RawMessage, []byte, int, error) {
		return json.RawMessage(`{"accessToken":"new","refreshToken":"new-r"}`), nil, http.StatusOK, nil
	})
	defer restore()

	response, err := refreshWorkbuddyCredential(&abi.Host{}, pluginapiAuthRefresh(t,
		`{"auth":{"accessToken":"old","refreshToken":"r","expiresAt":`+itoa(old)+`}}`))
	if err != nil {
		t.Fatalf("refreshWorkbuddyCredential: %v", err)
	}
	var sa storedAuth
	if err := json.Unmarshal(response.Auth.StorageJSON, &sa); err != nil {
		t.Fatal(err)
	}
	if sa.Auth.ExpiresAt != old {
		t.Fatalf("expiresAt = %d, want the previous %d", sa.Auth.ExpiresAt, old)
	}
}

// 远离到期时间的凭据不该触发任何续期调用——那是每个请求一次无谓的上游往返。
func TestEnsureCredentialLeavesFreshCredentialAlone(t *testing.T) {
	called := false
	restore := stubRefreshCall(func(*storedAuth) (json.RawMessage, []byte, int, error) {
		called = true
		return nil, nil, 200, nil
	})
	defer restore()

	storage := []byte(`{"auth":{"accessToken":"still-good","refreshToken":"r","expiresAt":` +
		itoa(time.Now().Add(2*time.Hour).Unix()) + `}}`)
	sa, err := ensureWorkbuddyCredential(&abi.Host{}, "workbuddy-uid.json", storage, nil)
	if err != nil {
		t.Fatalf("ensureWorkbuddyCredential: %v", err)
	}
	if called {
		t.Fatal("a comfortably valid credential must not be refreshed")
	}
	if sa.Auth.AccessToken != "still-good" {
		t.Fatalf("access token = %q", sa.Auth.AccessToken)
	}
}

func stubRefreshCall(fn func(*storedAuth) (json.RawMessage, []byte, int, error)) func() {
	previous := refreshCallSource
	refreshCallSource = fn
	return func() { refreshCallSource = previous }
}

func itoa(value int64) string {
	return strconv.FormatInt(value, 10)
}

// pluginapiAuthRefresh 把一段 storage JSON 包成刷新请求，省去每个用例重复构造。
func pluginapiAuthRefresh(t *testing.T, storage string) pluginapi.AuthRefreshRequest {
	t.Helper()
	return pluginapi.AuthRefreshRequest{StorageJSON: json.RawMessage(storage)}
}
