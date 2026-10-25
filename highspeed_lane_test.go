package main

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// ---- 高速卡 lane（highspeed.go）：取卡 → 带卡转发 → 失败落回常规通道 ----

// newHighspeedLaneAPI 组装 lane 测试环境：单一 httptest 上游按路径分派
// draw / highspeed messages / 常规 relay 三面，默认 lane 开启。
// 返回 (api, accountID, draw命中数, messages命中数, relay命中数)。
func newHighspeedLaneAPI(t *testing.T, drawHandler, msgHandler, relayHandler http.HandlerFunc) (*ZCodeAPI, int64, *atomic.Int32, *atomic.Int32, *atomic.Int32) {
	t.Helper()
	p, db := newPaidTestPool(t)
	a := mkDualAccount("hs-lane", StatusActive)
	id, err := db.UpsertAccount(a)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.SetSetting("highspeed_lane_enabled", "1"); err != nil {
		t.Fatal(err)
	}
	var draws, msgs, relays atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case strings.HasSuffix(r.URL.Path, "/highspeed/draw"):
			draws.Add(1)
			drawHandler(w, r)
		case strings.Contains(r.URL.Path, "/highspeed/anthropic"):
			msgs.Add(1)
			msgHandler(w, r)
		default:
			relays.Add(1)
			relayHandler(w, r)
		}
	}))
	t.Cleanup(server.Close)

	// URL 注入（包级 var，测试后还原）
	origDraw, origMsgs := highspeedDrawURL, highspeedMessagesURL
	highspeedDrawURL = server.URL + "/api/v1/highspeed/draw"
	highspeedMessagesURL = server.URL + "/api/v1/highspeed/anthropic/v1/messages"
	t.Cleanup(func() { highspeedDrawURL, highspeedMessagesURL = origDraw, origMsgs })

	cfg := &FileConfig{Upstream: UpstreamURLs{Zai: server.URL + "/api/v1/zcode-plan/anthropic/v1/messages", ZaiFallback: server.URL, Bigmodel: server.URL}}
	z := NewZCodeAPI(cfg, db, p, NewCaptchaService(cfg, db, "test"), "test")
	z.routing = &EndpointRouter{snapshot: &routingSnapshot{expiresAt: time.Now().Add(time.Hour)}}
	return z, id, &draws, &msgs, &relays
}

// highspeedRequest 以高速卡形状直调 HandleMessages（与生产到达形状一致）
func highspeedRequest(t *testing.T, z *ZCodeAPI, stream bool, body string) *httptest.ResponseRecorder {
	t.Helper()
	if body == "" {
		body = `{"model":"glm-5.1-highspeed","stream":` + boolJSON(stream) + `,"messages":[{"role":"user","content":"hello"}]}`
	}
	w := httptest.NewRecorder()
	z.HandleMessages(w, httptest.NewRequest(http.MethodPost, "/api/v1/highspeed/anthropic/v1/messages", strings.NewReader(body)))
	return w
}

func boolJSON(b bool) string {
	if b {
		return "true"
	}
	return "false"
}

// okDraw 返回标准取卡成功信封（卡面模型与请求不同，验证换发）
func okDraw(cardID string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var body map[string]interface{}
		_ = json.NewDecoder(r.Body).Decode(&body)
		if body["task_id"] == "" || body["provider"] != "builtin:zai-coding-plan" || body["model"] == "" {
			w.WriteHeader(http.StatusBadRequest)
			io.WriteString(w, `{"code":3001,"msg":"missing fields"}`)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		io.WriteString(w, `{"code":0,"msg":"success","data":{"card":{"card_id":"`+cardID+`","task_id":"t","provider":"zai","model":"GLM-5.1-Highspeed","issued_at":`+jsonInt64String(time.Now().UnixMilli())+`,"expires_at":`+jsonInt64String(time.Now().Add(10*time.Minute).UnixMilli())+`},"next_draw_at":0}}`)
	}
}

func jsonInt64String(n int64) string {
	b, _ := json.Marshal(n)
	return string(b)
}

// sseMessages 返回最小合法 Anthropic SSE 流（extra 非 nil 时完全接管响应）
func sseMessages(extra http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if extra != nil {
			extra(w, r)
			return
		}
		if r.Header.Get("X-Highspeed-Card-ID") == "" {
			w.WriteHeader(http.StatusBadRequest)
			io.WriteString(w, `{"code":3001,"msg":"missing card id"}`)
			return
		}
		w.Header().Set("Content-Type", "text/event-stream")
		io.WriteString(w, "event: message_start\ndata: {\"type\":\"message_start\",\"message\":{\"id\":\"msg_hs\",\"model\":\"GLM-5.1-Highspeed\",\"usage\":{\"input_tokens\":7}}}\n\n")
		io.WriteString(w, "event: content_block_start\ndata: {\"type\":\"content_block_start\",\"index\":0,\"content_block\":{\"type\":\"text\",\"text\":\"\"}}\n\n")
		io.WriteString(w, "event: content_block_delta\ndata: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"text_delta\",\"text\":\"HS-OK\"}}\n\n")
		io.WriteString(w, "event: content_block_stop\ndata: {\"type\":\"content_block_stop\",\"index\":0}\n\n")
		io.WriteString(w, "event: message_delta\ndata: {\"type\":\"message_delta\",\"delta\":{\"stop_reason\":\"end_turn\"},\"usage\":{\"output_tokens\":3}}\n\n")
		io.WriteString(w, "event: message_stop\ndata: {\"type\":\"message_stop\"}\n\n")
	}
}

func relayOK(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/event-stream")
	io.WriteString(w, "event: message_start\ndata: {\"type\":\"message_start\",\"message\":{\"id\":\"msg_relay\",\"model\":\"GLM-5.3\",\"usage\":{\"input_tokens\":7}}}\n\n")
	io.WriteString(w, "event: content_block_delta\ndata: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"text_delta\",\"text\":\"RELAY-OK\"}}\n\n")
	io.WriteString(w, "event: message_delta\ndata: {\"type\":\"message_delta\",\"delta\":{\"stop_reason\":\"end_turn\"},\"usage\":{\"output_tokens\":2}}\n\n")
	io.WriteString(w, "event: message_stop\ndata: {\"type\":\"message_stop\"}\n\n")
}

// 开关关闭：draw 不被打到，请求原样走常规通道（wave-1 别名语义保持）
func TestHighspeedLaneDisabledFallsBackToRelay(t *testing.T) {
	z, _, draws, _, relays := newHighspeedLaneAPI(t, okDraw("hsc_x"), sseMessages(nil), relayOK)
	if err := z.db.SetSetting("highspeed_lane_enabled", "0"); err != nil {
		t.Fatal(err)
	}
	w := highspeedRequest(t, z, true, "")
	if !strings.Contains(w.Body.String(), "RELAY-OK") {
		t.Fatalf("disabled lane must fall back to relay, got %s", w.Body.String())
	}
	if n := draws.Load(); n != 0 {
		t.Fatalf("draw hits = %d, want 0 (lane disabled)", n)
	}
	if n := relays.Load(); n != 1 {
		t.Fatalf("relay hits = %d, want 1", n)
	}
}

// 开心路径：取卡 → 卡面模型换发 → 卡号头 → SSE 透传，账号分毫不动
func TestHighspeedLaneHappyPath(t *testing.T) {
	z, id, draws, msgs, _ := newHighspeedLaneAPI(t, okDraw("hsc_1"), sseMessages(nil), relayOK)
	w := highspeedRequest(t, z, true, "")
	if !strings.Contains(w.Body.String(), "HS-OK") {
		t.Fatalf("lane response missing HS-OK, got %s", w.Body.String())
	}
	if !strings.Contains(w.Header().Get("Content-Type"), "text/event-stream") {
		t.Fatalf("streaming client must get SSE, got %s", w.Header().Get("Content-Type"))
	}
	if n := draws.Load(); n != 1 {
		t.Fatalf("draw hits = %d, want 1", n)
	}
	if n := msgs.Load(); n != 1 {
		t.Fatalf("messages hits = %d, want 1", n)
	}
	fresh, err := z.db.GetAccount(id)
	if err != nil {
		t.Fatal(err)
	}
	if fresh.FailCount != 0 || fresh.CoolingUntil != 0 || fresh.Status != StatusActive {
		t.Fatalf("happy path must not touch account state, got fail=%d cooling=%d status=%s",
			fresh.FailCount, fresh.CoolingUntil, fresh.Status)
	}
}

// 卡面模型换发 + 鉴权头：上游收到的 body.model = 卡面模型，Authorization/卡号头齐备
func TestHighspeedLaneCardModelSubstitutionAndHeaders(t *testing.T) {
	var gotModel, gotCard, gotAuth string
	msgs := sseMessages(func(w http.ResponseWriter, r *http.Request) {
		var body map[string]interface{}
		_ = json.NewDecoder(r.Body).Decode(&body)
		gotModel, _ = body["model"].(string)
		gotCard = r.Header.Get("X-Highspeed-Card-ID")
		gotAuth = r.Header.Get("Authorization")
		w.Header().Set("Content-Type", "text/event-stream")
		io.WriteString(w, "event: message_start\ndata: {\"type\":\"message_start\",\"message\":{\"id\":\"m\",\"model\":\"GLM-5.1-Highspeed\",\"usage\":{\"input_tokens\":1}}}\n\n")
		io.WriteString(w, "event: message_delta\ndata: {\"type\":\"message_delta\",\"delta\":{\"stop_reason\":\"end_turn\"},\"usage\":{\"output_tokens\":1}}\n\n")
		io.WriteString(w, "event: message_stop\ndata: {\"type\":\"message_stop\"}\n\n")
	})
	z, _, _, _, _ := newHighspeedLaneAPI(t, okDraw("hsc_9"), msgs, relayOK)
	highspeedRequest(t, z, true, "")
	if gotModel != "GLM-5.1-Highspeed" {
		t.Fatalf("upstream model = %q, want card model GLM-5.1-Highspeed (official modelSelection substitution)", gotModel)
	}
	if gotCard != "hsc_9" {
		t.Fatalf("X-Highspeed-Card-ID = %q, want hsc_9", gotCard)
	}
	if gotAuth != "Bearer jwt-hs-lane" {
		t.Fatalf("Authorization = %q, want pool JWT bearer", gotAuth)
	}
}

// 3402 卡过期：重取一次重试成功（draws=2, msgs=2）
func TestHighspeedLane3402RedrawsOnce(t *testing.T) {
	var calls atomic.Int32
	msgs := sseMessages(func(w http.ResponseWriter, r *http.Request) {
		if calls.Add(1) == 1 {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusBadRequest)
			io.WriteString(w, `{"error":{"provider_code":"3402","message":"card expired"}}`)
			return
		}
		sseMessages(nil)(w, r)
	})
	z, _, draws, _, _ := newHighspeedLaneAPI(t, okDraw("hsc_a"), msgs, relayOK)
	w := highspeedRequest(t, z, true, "")
	if !strings.Contains(w.Body.String(), "HS-OK") {
		t.Fatalf("after redraw the request must succeed, got %s", w.Body.String())
	}
	if n := draws.Load(); n != 2 {
		t.Fatalf("draw hits = %d, want 2 (initial + redraw)", n)
	}
	if n := calls.Load(); n != 2 {
		t.Fatalf("messages hits = %d, want 2 (3402 + retry)", n)
	}
}

// 取卡业务码失败（资格/配额类）：落回常规通道、账号不惩罚、lane 进入静默期
// （第二个请求不再打 draw）
func TestHighspeedLaneDrawBusinessErrorFallsBack(t *testing.T) {
	z, id, draws, _, relays := newHighspeedLaneAPI(t,
		func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			io.WriteString(w, `{"code":3103,"msg":"not entitled"}`)
		}, sseMessages(nil), relayOK)
	w := highspeedRequest(t, z, true, "")
	if !strings.Contains(w.Body.String(), "RELAY-OK") {
		t.Fatalf("draw failure must fall back to relay, got %s", w.Body.String())
	}
	fresh, err := z.db.GetAccount(id)
	if err != nil {
		t.Fatal(err)
	}
	if fresh.FailCount != 0 || fresh.CoolingUntil != 0 || fresh.Status != StatusActive {
		t.Fatalf("draw business failure must not penalize account, got fail=%d cooling=%d status=%s",
			fresh.FailCount, fresh.CoolingUntil, fresh.Status)
	}
	// 静默期内第二个请求：不打 draw，直接落回
	w2 := highspeedRequest(t, z, true, "")
	if !strings.Contains(w2.Body.String(), "RELAY-OK") {
		t.Fatalf("second request must also fall back, got %s", w2.Body.String())
	}
	if n := draws.Load(); n != 1 {
		t.Fatalf("draw hits = %d, want 1 (draw-block silence window)", n)
	}
	if n := relays.Load(); n != 2 {
		t.Fatalf("relay hits = %d, want 2", n)
	}
}

// 卡面缓存：TTL 内第二个请求不重取卡
func TestHighspeedLaneCardCache(t *testing.T) {
	z, _, draws, msgs, _ := newHighspeedLaneAPI(t, okDraw("hsc_c"), sseMessages(nil), relayOK)
	highspeedRequest(t, z, true, "")
	highspeedRequest(t, z, true, "")
	if n := draws.Load(); n != 1 {
		t.Fatalf("draw hits = %d, want 1 (card cached)", n)
	}
	if n := msgs.Load(); n != 2 {
		t.Fatalf("messages hits = %d, want 2", n)
	}
}

// 非流式客户端：上游强制流式 → 网关聚合为完整 message
func TestHighspeedLaneNonStreamAggregation(t *testing.T) {
	z, _, _, _, _ := newHighspeedLaneAPI(t, okDraw("hsc_d"), sseMessages(nil), relayOK)
	w := highspeedRequest(t, z, false, "")
	if !strings.Contains(w.Header().Get("Content-Type"), "application/json") {
		t.Fatalf("non-stream client must get JSON, got %s", w.Header().Get("Content-Type"))
	}
	var msg map[string]interface{}
	if err := json.Unmarshal(w.Body.Bytes(), &msg); err != nil {
		t.Fatalf("aggregated body not JSON: %v; body=%s", err, w.Body.String())
	}
	if msg["id"] != "msg_hs" {
		t.Fatalf("aggregated message id = %v, want msg_hs", msg["id"])
	}
	content, _ := msg["content"].([]interface{})
	if len(content) == 0 {
		t.Fatalf("aggregated message missing content blocks: %s", w.Body.String())
	}
}

// 无 lane 资格账号（仅 APIKey、无 JWT）：lane 放行落回常规通道（付费侧照常服务）
func TestHighspeedLaneNoAccountFallsBack(t *testing.T) {
	p, db := newPaidTestPool(t)
	a := mkDualAccount("hs-off", StatusActive)
	a.ZCodeJWT = "" // lane 选号要求 JWT：该账号对 lane 不可见，relay 走付费通道
	if _, err := db.UpsertAccount(a); err != nil {
		t.Fatal(err)
	}
	_ = db.SetSetting("highspeed_lane_enabled", "1")
	var draws atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "/d") {
			draws.Add(1)
		}
		relayOK(w, r)
	}))
	t.Cleanup(server.Close)
	origDraw, origMsgs := highspeedDrawURL, highspeedMessagesURL
	highspeedDrawURL, highspeedMessagesURL = server.URL+"/d", server.URL+"/m"
	t.Cleanup(func() { highspeedDrawURL, highspeedMessagesURL = origDraw, origMsgs })
	cfg := &FileConfig{Upstream: UpstreamURLs{Zai: server.URL, ZaiFallback: server.URL, Bigmodel: server.URL}}
	z := NewZCodeAPI(cfg, db, p, NewCaptchaService(cfg, db, "test"), "test")
	z.routing = &EndpointRouter{snapshot: &routingSnapshot{expiresAt: time.Now().Add(time.Hour)}}

	w := highspeedRequest(t, z, true, "")
	if !strings.Contains(w.Body.String(), "RELAY-OK") {
		t.Fatalf("no-account lane must fall back to relay, got %s", w.Body.String())
	}
	if n := draws.Load(); n != 0 {
		t.Fatalf("draw hits = %d, want 0 (no eligible account)", n)
	}
}

// 上游 next_draw_at 冷却必须生效（评审 Medium 1 回归）：业务码失败体携带
// data.next_draw_at 时，lane 静默期取其值（钳制下限 1min），而非固定 10min。
// 时长用 until−t0 度量（t0 = 请求前）：until 是"置位时刻+block"，与置位后
// 套件负载造成的耗时无关——用 time.Until 断言会在慢跑下假失败
func TestHighspeedLaneNextDrawAtHonored(t *testing.T) {
	z, id, _, _, _ := newHighspeedLaneAPI(t,
		func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			io.WriteString(w, `{"code":3103,"msg":"quota","data":{"next_draw_at":`+jsonInt64String(time.Now().Add(90*time.Second).UnixMilli())+`}}`)
		}, sseMessages(nil), relayOK)
	t0 := time.Now()
	highspeedRequest(t, z, true, "")
	z.hsMu.Lock()
	until := z.hsDrawBlockUntil[id]
	z.hsMu.Unlock()
	if until.IsZero() {
		t.Fatalf("draw block must be set")
	}
	d := until.Sub(t0)
	if d < 60*time.Second || d > 2*time.Minute {
		t.Fatalf("block = %v, want ~90s (next_draw_at clamped to [1m,1h]); fixed 10m block means next_draw_at was ignored", d.Truncate(time.Second))
	}
}

// 401 壳携带风控 3012 体（兄弟端点已实证的形状）绝不能 MarkInvalid（评审
// Medium 2 回归）：预分类命中 → 短冷却 + 落回，账号保持 active
func TestHighspeedLaneDraw3012ShellNotInvalid(t *testing.T) {
	z, id, _, _, relays := newHighspeedLaneAPI(t,
		func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusUnauthorized)
			io.WriteString(w, `{"code":3012,"msg":"unusual activity"}`)
		}, sseMessages(nil), relayOK)
	w := highspeedRequest(t, z, true, "")
	if !strings.Contains(w.Body.String(), "RELAY-OK") {
		t.Fatalf("risk-blocked draw must fall back to relay, got %s", w.Body.String())
	}
	fresh, err := z.db.GetAccount(id)
	if err != nil {
		t.Fatal(err)
	}
	if fresh.Status == StatusInvalid {
		t.Fatalf("3012-in-401-shell must never MarkInvalid, got status=%s err=%q", fresh.Status, fresh.LastError)
	}
	if fresh.CoolingUntil == 0 {
		t.Fatalf("risk block should cool the account briefly, cooling=0")
	}
	if n := relays.Load(); n != 1 {
		t.Fatalf("relay hits = %d, want 1", n)
	}
}

// 覆盖面闸（官方 isModelCovered）：非高速卡模型不取卡，直接落回常规通道
// （回归：未覆盖模型 draw 曾被上游 3001 parameter error 拒绝并空烧 10min 静默）
func TestHighspeedLaneUncoveredModelSkipsDraw(t *testing.T) {
	z, _, draws, _, relays := newHighspeedLaneAPI(t, okDraw("hsc_u"), sseMessages(nil), relayOK)
	w := highspeedRequest(t, z, true, `{"model":"GLM-5.3","stream":true,"messages":[{"role":"user","content":"hi"}]}`)
	if !strings.Contains(w.Body.String(), "RELAY-OK") {
		t.Fatalf("uncovered model must fall back to relay, got %s", w.Body.String())
	}
	if n := draws.Load(); n != 0 {
		t.Fatalf("draw hits = %d, want 0 (uncovered model never draws)", n)
	}
	if n := relays.Load(); n != 1 {
		t.Fatalf("relay hits = %d, want 1", n)
	}
}

// 高速卡上游消息端点 429（卡/端点饱和）：落回常规通道，账号不惩罚
func TestHighspeedLaneUpstream429FallsBack(t *testing.T) {
	msgs := sseMessages(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusTooManyRequests)
		io.WriteString(w, `{"code":3105,"msg":"saturated"}`)
	})
	z, id, _, _, relays := newHighspeedLaneAPI(t, okDraw("hsc_e"), msgs, relayOK)
	w := highspeedRequest(t, z, true, "")
	if !strings.Contains(w.Body.String(), "RELAY-OK") {
		t.Fatalf("upstream 429 must fall back to relay, got %s", w.Body.String())
	}
	fresh, err := z.db.GetAccount(id)
	if err != nil {
		t.Fatal(err)
	}
	if fresh.CoolingUntil != 0 || fresh.Status != StatusActive {
		t.Fatalf("lane 429 must not cool the account, got cooling=%d status=%s", fresh.CoolingUntil, fresh.Status)
	}
	if n := relays.Load(); n != 1 {
		t.Fatalf("relay hits = %d, want 1", n)
	}
}
