package main

import (
	"context"
	"encoding/json"
	"math/rand"
	"sync"
	"time"

	"github.com/coder/websocket"
)

// wireMsg is the message schema shared by hello and tick.
type wireMsg struct {
	T        string `json:"t"`
	Gen      string `json:"gen"`
	Seq      int64  `json:"seq"`
	BootAt   int64  `json:"bootAt"`
	Colo     string `json:"colo"`
	EdgeColo string `json:"edgeColo"`
	Conns    int    `json:"conns"`
	Now      int64  `json:"now"`
}

// Monitor watches one DO through N connections.
//
// 判定ロジック:
//   - tick が tickTimeout 途切れる or close → 接続喪失（この時点では死亡未確定）
//   - 再接続後の最初のメッセージの gen を照合
//   - gen が変わっていた → DO死（do_death、モニタ単位で新genごとに1回だけ発行）
//   - gen が同じ → 経路の問題（path_blip、DOは生きていた）
type Monitor struct {
	cfg    *Config
	doName string
	cohort string
	events *EventWriter
	stats  *DoStats

	mu       sync.Mutex
	lastGen  string
	genBoot  int64
	colo     string
	seenGens map[string]bool
	conns    []*Conn
}

type Conn struct {
	m          *Monitor
	idx        int
	lastGen    string
	connected  bool
	lost       bool
	lostAt     time.Time
	lostWhy    string
	failStreak int
}

func NewMonitor(cfg *Config, doName, cohort string, events *EventWriter, stats *DoStats, nConns int) *Monitor {
	m := &Monitor{
		cfg: cfg, doName: doName, cohort: cohort,
		events: events, stats: stats,
		seenGens: map[string]bool{},
	}
	for i := 0; i < nConns; i++ {
		m.conns = append(m.conns, &Conn{m: m, idx: i})
	}
	return m
}

func (m *Monitor) Run(ctx context.Context, wg *sync.WaitGroup) {
	for _, c := range m.conns {
		wg.Add(1)
		go func(c *Conn) {
			defer wg.Done()
			c.run(ctx)
		}(c)
	}
}

func (c *Conn) run(ctx context.Context) {
	url, err := c.m.cfg.wsURL(c.m.doName)
	if err != nil {
		return
	}
	// 起動時は rampUp 窓に分散して接続する（1.5万本の一斉ダイヤルを避ける）
	sleepCtx(ctx, time.Duration(rand.Int63n(int64(c.m.cfg.RampUpSeconds)+1))*time.Second)

	for ctx.Err() == nil {
		dialCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
		ws, _, err := websocket.Dial(dialCtx, url, nil)
		cancel()
		if err != nil {
			c.m.stats.DialFails.Add(1)
			c.failStreak++
			sleepCtx(ctx, c.m.backoff(c.failStreak))
			continue
		}
		ws.SetReadLimit(1 << 20)
		wasIdentified := c.readLoop(ctx, ws)
		ws.Close(websocket.StatusNormalClosure, "")
		// 大量同時切断後の一斉リトライは TLS ハンドシェイクで CPU を飽和させ、
		// 誰も繋がらない輻輳崩壊に陥る。指数バックオフで攻撃的リトライを
		// 減衰させ、ストームを必ず収束させる。
		c.failStreak++
		if wasIdentified && c.failStreak == 1 && c.m.cfg.ReconnectSpreadMs > 0 && len(c.m.conns) >= 100 {
			// 分散は再接続ストームになりうる大口モニタのみ。少数接続のDO
			// (Sコホート等)は即時再接続し、CF死の復旧時間を正確に測る。
			// 確立済み接続の喪失（＝DO死/大量切断の可能性）: 再接続ストームで
			// DOを過負荷にしないよう時間窓に分散する
			sleepCtx(ctx, time.Duration(rand.Intn(c.m.cfg.ReconnectSpreadMs))*time.Millisecond)
		} else {
			sleepCtx(ctx, c.m.backoff(c.failStreak))
		}
	}
}

func (c *Conn) readLoop(ctx context.Context, ws *websocket.Conn) bool {
	timeout := time.Duration(c.m.cfg.TickTimeoutMs) * time.Millisecond
	identified := false
	for {
		rctx, cancel := context.WithTimeout(ctx, timeout)
		_, data, err := ws.Read(rctx)
		cancel()
		if err != nil {
			if ctx.Err() != nil {
				return identified // shutdown中は喪失として扱わない
			}
			c.m.onLost(c, classifyErr(err))
			return identified
		}
		if identified {
			// 世代IDの変化は必ずプロセス再起動＝切断を伴うため、
			// 2通目以降のtickはパース不要。大量接続時のCPU支配項になるので
			// 受信カウントだけ更新する。
			c.m.stats.Ticks.Add(1)
			continue
		}
		var msg wireMsg
		if json.Unmarshal(data, &msg) != nil || msg.Gen == "" {
			continue
		}
		c.m.onIdentity(c, &msg)
		identified = true
	}
}

func (m *Monitor) onIdentity(c *Conn, msg *wireMsg) {
	now := time.Now()
	m.mu.Lock()
	defer m.mu.Unlock()

	switch {
	case m.lastGen == "":
		m.lastGen, m.genBoot, m.colo = msg.Gen, msg.BootAt, msg.Colo
		m.seenGens[msg.Gen] = true
		m.events.Emit("gen_first_seen", map[string]any{
			"do": m.doName, "cohort": m.cohort,
			"gen": msg.Gen, "bootAt": msg.BootAt, "colo": msg.Colo, "edgeColo": msg.EdgeColo,
		})
	case msg.Gen != m.lastGen && !m.seenGens[msg.Gen]:
		m.seenGens[msg.Gen] = true
		firstLost, nLost := now, 0
		lossReasons := map[string]int{}
		for _, c2 := range m.conns {
			if c2.lost {
				nLost++
				lossReasons[c2.lostWhy]++
				if c2.lostAt.Before(firstLost) {
					firstLost = c2.lostAt
				}
			}
		}
		m.events.Emit("do_death", map[string]any{
			"do": m.doName, "cohort": m.cohort,
			"oldGen": m.lastGen, "newGen": msg.Gen,
			"oldColo": m.colo, "newColo": msg.Colo,
			"oldGenBootAt": m.genBoot,
			"firstLostAt":  firstLost.UTC().Format(time.RFC3339Nano),
			"recoveredAt":  now.UTC().Format(time.RFC3339Nano),
			"uptimeMs":     firstLost.UnixMilli() - m.genBoot,
			"downtimeMs":   now.Sub(firstLost).Milliseconds(),
			"connsLost":    nLost,
			// CF死のときサーバーがcloseフレームを送るのか(=どのcloseコードか)、
			// それとも無言死(タイムアウト)なのかを判別するための切断理由内訳
			"lossReasons": lossReasons,
		})
		m.stats.Deaths.Add(1)
		m.lastGen, m.genBoot, m.colo = msg.Gen, msg.BootAt, msg.Colo
	}

	if c.lost {
		gap := now.Sub(c.lostAt).Milliseconds()
		if c.lastGen == msg.Gen {
			// このconnが最後に見たgenのまま = DOは生きていた = 経路側の問題
			m.stats.Blips.Add(1)
			m.stats.ObserveBlipGap(gap)
			m.events.Emit("path_blip", map[string]any{
				"do": m.doName, "cohort": m.cohort, "conn": c.idx,
				"gen": msg.Gen, "gapMs": gap, "reason": c.lostWhy,
			})
		} else {
			m.stats.ReconnectMsSum.Add(gap)
			m.stats.ReconnectN.Add(1)
		}
		c.lost = false
	}
	if !c.connected {
		c.connected = true
		m.stats.Connected.Add(1)
	}
	c.failStreak = 0
	c.lastGen = msg.Gen
}

func (m *Monitor) onLost(c *Conn, reason string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if c.connected {
		c.connected = false
		m.stats.Connected.Add(-1)
	}
	c.lost = true
	c.lostAt = time.Now()
	c.lostWhy = reason
}

// backoff returns full-jitter exponential backoff: streak 1 は従来どおり
// 素早く(復旧時間の測定精度を保つ)、連続失敗で指数的に伸ばして
// 30秒を上限とする。
func (m *Monitor) backoff(streak int) time.Duration {
	lo, hi := m.cfg.ReconnectMinMs, m.cfg.ReconnectMaxMs
	if hi <= lo {
		hi = lo + 1
	}
	base := time.Duration(lo+rand.Intn(hi-lo)) * time.Millisecond
	if streak <= 1 {
		return base
	}
	shift := streak - 1
	if shift > 5 {
		shift = 5 // 最大 32 倍
	}
	d := base << shift
	const maxBackoff = 30 * time.Second
	if d > maxBackoff {
		d = maxBackoff
	}
	// full jitter: 0〜d の一様乱数で同期化を防ぐ
	return time.Duration(rand.Int63n(int64(d)))
}

func classifyErr(err error) string {
	status := websocket.CloseStatus(err)
	if status != -1 {
		return status.String()
	}
	if e, ok := err.(interface{ Timeout() bool }); ok && e.Timeout() {
		return "tick_timeout"
	}
	if ctxErr := context.DeadlineExceeded; err == ctxErr {
		return "tick_timeout"
	}
	// context.DeadlineExceeded がラップされているケース
	if s := err.Error(); len(s) > 120 {
		return s[:120]
	} else {
		return s
	}
}

func sleepCtx(ctx context.Context, d time.Duration) {
	if d <= 0 {
		return
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
	case <-t.C:
	}
}
