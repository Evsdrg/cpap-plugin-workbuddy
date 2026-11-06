// host_bridge.go 把所有上游 HTTP 调用收敛到宿主 http 桥
// （host.http.do / host.http.do_stream / stream_read / stream_close）。
//
// 生产路径一律经宿主转发，这样请求日志能捕获出站调用，宿主的传输策略
// （代理、超时、TLS 指纹）也才会生效。
//
// 这里的每个函数都要求调用方传入本次调用的 *abi.Host：它携带宿主为本次
// 调用签发的 host_callback_id，出站回调必须原样带回，否则宿主无法把这次
// 出站归因到发起它的请求，用量面板里的「上游接口」列就会退化成入站协议。
//
// 唯一不走宿主的情形是单元测试（hostAPI == nil）。那时没有 cgo 宿主可调，
// 函数退化为直连本地 httptest 服务器。这是测试专用通道，生产永远不会命中。
package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sync"
	"time"

	"github.com/Evsdrg/cepvor-cpa-plinth/abi"
	"github.com/router-for-me/CLIProxyAPI/v8/sdk/pluginabi"
	"github.com/router-for-me/CLIProxyAPI/v8/sdk/pluginapi"
)

// sharedHTTPClient 是测试专用直连通道使用的 HTTP 客户端。
//
// 生产的上游调用一律经 host.HTTPDo / host.HTTPDoStream，由宿主的客户端
// 发出，因此这里不参与生产流量。新代码若在非测试路径直接用它，属于绕过
// 宿主传输策略的缺陷。
func sharedHTTPClient() *http.Client {
	httpClientOnce.Do(func() {
		// 不设 cookie jar：鉴权靠 Bearer 头，共享 jar 会让上游的 set-cookie
		// 状态在多账号之间串味。只有短命的登录客户端才配 jar。
		sharedClient = &http.Client{
			Timeout: 120 * time.Second,
			Transport: &http.Transport{
				MaxIdleConns:        20,
				IdleConnTimeout:     90 * time.Second,
				MaxIdleConnsPerHost: 5,
			},
		}
	})
	return sharedClient
}

// hostHTTPResponse 是插件侧对宿主 HTTP 响应的视图。Body 已完整缓冲，
// 与历史上 billing / models / usage 调用点 io.ReadAll 的用法一致。
type hostHTTPResponse struct {
	StatusCode int
	Headers    http.Header
	Body       []byte
}

// hostBridgeAvailable 报告宿主 http 桥是否可用。单元测试里为 false，
// 除非测试显式用 installFakeHostAPI 打开（那样 SDK 的 host 传输已被替换成
// 记录器，可以观察生产代码构造的 wire）。
func hostBridgeAvailable() bool {
	if hostBridgeForced {
		return true
	}
	return hostAPI != nil && hostAPI.call != nil
}

// hostBridgeForced 只由测试设置，让 hostHTTPDo / hostHTTPDoStream 走宿主
// 分支（实际执行的是测试替换过的 SDK 传输）。
var hostBridgeForced bool

// readRequestBody 读尽并关闭请求体，供宿主桥与直连通道复用。
func readRequestBody(req *http.Request) ([]byte, error) {
	if req == nil || req.Body == nil {
		return nil, nil
	}
	body, err := io.ReadAll(req.Body)
	if err != nil {
		return nil, fmt.Errorf("read request body: %w", err)
	}
	_ = req.Body.Close()
	return body, nil
}

// hostHTTPDo 经宿主 http 桥发出一次非流式请求。
//
// 生产路径调用 h.HTTPDo；它会把 h.CallbackID 一并送到宿主，宿主据此把
// 出站路径记到本次请求的用量上。测试路径（hostAPI == nil）直连本地服务器。
func hostHTTPDo(req *http.Request) (*hostHTTPResponse, error) {
	if req == nil {
		return nil, fmt.Errorf("nil request")
	}
	bodyBytes, err := readRequestBody(req)
	if err != nil {
		return nil, err
	}
	if !hostBridgeAvailable() {
		return hostHTTPDoDirect(req, bodyBytes)
	}
	h := hostFromContext(req.Context())
	if h == nil {
		h = &abi.Host{}
	}
	resp, errDo := h.HTTPDo(abi.HTTPDoRequest{
		Method:  req.Method,
		URL:     req.URL.String(),
		Headers: req.Header,
		Body:    bodyBytes,
	})
	if errDo != nil {
		return nil, errDo
	}
	return &hostHTTPResponse{
		StatusCode: resp.StatusCode,
		Headers:    resp.Headers,
		Body:       resp.Body,
	}, nil
}

// hostHTTPDoDirect 用插件自己的 http.Client 发请求。仅测试路径会走到。
func hostHTTPDoDirect(req *http.Request, bodyBytes []byte) (*hostHTTPResponse, error) {
	newReq, err := http.NewRequestWithContext(req.Context(), req.Method, req.URL.String(), bytes.NewReader(bodyBytes))
	if err != nil {
		return nil, err
	}
	newReq.Header = req.Header.Clone()
	resp, err := sharedHTTPClient().Do(newReq)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	return &hostHTTPResponse{
		StatusCode: resp.StatusCode,
		Headers:    resp.Header.Clone(),
		Body:       raw,
	}, nil
}

// upstreamHTTPStream 是流式上游的统一读取接口。
//
// Read 返回 (payload, done, err)：done 表示流正常结束，err 表示上游或桥
// 出错。宿主按任意大小的块推送（不保证按 SSE 行切分），所以行重组由消费
// 方负责。
type upstreamHTTPStream interface {
	Read() ([]byte, bool, error)
	Close()
}

// hostHTTPStream 是宿主桥返回的流句柄。
type hostHTTPStream struct {
	mu       sync.Mutex
	streamID string
	closed   bool
	inner    *abi.HTTPStream
	// direct / directAt 只在测试通道（hostAPI == nil）使用：完整响应体被
	// 缓冲在 direct 里，directAt 记录已读到的位置。
	direct   []byte
	directAt int
	// readChunk / closeFn 是流数据来源与关闭动作的注入点。生产为
	// inner.Read / inner.Close；测试替换它们来模拟阻塞读取，验证 Close 能
	// 解除阻塞。两者都为 nil 时走 inner。
	readChunk func() ([]byte, error)
	closeFn   func()
}

// hostHTTPDoStream 经宿主 http 桥打开一次流式请求。
//
// 生产路径调用 h.HTTPDoStream；h.CallbackID 让宿主把这次出站记到本次请求
// 的用量上。测试路径直连本地服务器并把完整响应缓冲成内存流。
func hostHTTPDoStream(req *http.Request) (*hostHTTPStream, int, http.Header, error) {
	if req == nil {
		return nil, 0, nil, fmt.Errorf("nil request")
	}
	bodyBytes, err := readRequestBody(req)
	if err != nil {
		return nil, 0, nil, err
	}
	if !hostBridgeAvailable() {
		return hostHTTPDoStreamDirect(req, bodyBytes)
	}
	h := hostFromContext(req.Context())
	if h == nil {
		h = &abi.Host{}
	}
	stream, errOpen := h.HTTPDoStream(abi.HTTPDoRequest{
		Method:  req.Method,
		URL:     req.URL.String(),
		Headers: req.Header,
		Body:    bodyBytes,
	})
	if errOpen != nil {
		return nil, 0, nil, errOpen
	}
	return &hostHTTPStream{streamID: stream.StreamID, inner: stream}, stream.StatusCode, stream.Headers, nil
}

// hostHTTPDoStreamDirect 是测试专用通道：用插件自己的 http.Client 发请求，
// 把完整响应体缓冲成内存流，使 Read/Close 保持同样的契约。
func hostHTTPDoStreamDirect(req *http.Request, bodyBytes []byte) (*hostHTTPStream, int, http.Header, error) {
	newReq, err := http.NewRequestWithContext(req.Context(), req.Method, req.URL.String(), bytes.NewReader(bodyBytes))
	if err != nil {
		return nil, 0, nil, err
	}
	newReq.Header = req.Header.Clone()
	resp, err := sharedHTTPClient().Do(newReq)
	if err != nil {
		return nil, 0, nil, err
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, resp.StatusCode, resp.Header.Clone(), err
	}
	return &hostHTTPStream{direct: raw}, resp.StatusCode, resp.Header.Clone(), nil
}

// Read 拉取下一块。返回 (payload, done, err)。
func (s *hostHTTPStream) Read() ([]byte, bool, error) {
	if s == nil {
		return nil, true, fmt.Errorf("stream closed")
	}
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil, true, fmt.Errorf("stream closed")
	}
	if s.direct != nil {
		if s.directAt >= len(s.direct) {
			s.mu.Unlock()
			return nil, true, nil
		}
		out := append([]byte(nil), s.direct[s.directAt:]...)
		s.directAt = len(s.direct)
		s.mu.Unlock()
		return out, false, nil
	}
	inner := s.inner
	readChunk := s.readChunk
	s.mu.Unlock()
	if readChunk == nil {
		if inner == nil {
			return nil, true, fmt.Errorf("stream closed")
		}
		readChunk = inner.Read
	}
	payload, errRead := readChunk()
	if errRead == io.EOF {
		return nil, true, nil
	}
	if errRead != nil {
		return nil, true, errRead
	}
	return payload, false, nil
}

// Close 中止上游流。与 Read 并发调用是安全的。
func (s *hostHTTPStream) Close() {
	if s == nil {
		return
	}
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return
	}
	s.closed = true
	inner := s.inner
	closeFn := s.closeFn
	s.inner = nil
	s.readChunk = nil
	s.closeFn = nil
	s.direct = nil
	s.directAt = 0
	s.mu.Unlock()
	if closeFn != nil {
		closeFn()
		return
	}
	if inner != nil {
		_ = inner.Close()
	}
}

// hostStreamReader 把 upstreamHTTPStream 适配成 io.Reader，使既有的
// bufio.Scanner / io.ReadAll 调用点无需改动。
type hostStreamReader struct {
	s    upstreamHTTPStream
	buf  []byte // 上一块未消费完的字节
	done bool
	err  error
}

func newHostStreamReader(s upstreamHTTPStream) *hostStreamReader {
	return &hostStreamReader{s: s}
}

func (r *hostStreamReader) Read(p []byte) (int, error) {
	if len(r.buf) > 0 {
		n := copy(p, r.buf)
		r.buf = r.buf[n:]
		return n, nil
	}
	if r.done {
		if r.err != nil {
			return 0, r.err
		}
		return 0, io.EOF
	}
	chunk, done, err := r.s.Read()
	if err != nil {
		r.done = true
		r.err = err
		return 0, err
	}
	if len(chunk) > 0 {
		n := copy(p, chunk)
		if n < len(chunk) {
			r.buf = append(r.buf, chunk[n:]...)
		}
		if done {
			r.done = true
		}
		return n, nil
	}
	if done {
		r.done = true
		return 0, io.EOF
	}
	return r.Read(p)
}

// mustJSON 序列化 v，失败即 panic——这些 wire 结构体必然可序列化，
// 失败只可能是编程错误。
func mustJSON(v any) []byte {
	b, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}
	return b
}

// 编译期断言：pluginabi 与 pluginapi 在本文件仍有引用（供 host_bridge 的
// 其余调用点使用），避免 import 被误删。
var _ = pluginabi.MethodHostHTTPDo
var _ = pluginapi.HTTPResponse{}
