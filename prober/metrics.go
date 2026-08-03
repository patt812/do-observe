package main

import (
	"fmt"
	"net/http"
	"sort"
	"sync/atomic"
)

// DoStats holds per-DO counters. Prometheus text format を手書きで吐くだけなので
// client_golang には依存しない。
// blipGapBuckets: blip復帰時間ヒストグラムのバケット上限(秒)。
// 「一定秒数内に再接続できたか」のSLO判定に使う。
var blipGapBuckets = []float64{1, 2, 3, 5, 10, 30}

type DoStats struct {
	Cohort         string
	Connected      atomic.Int64
	Ticks          atomic.Int64
	Deaths         atomic.Int64
	Blips          atomic.Int64
	DialFails      atomic.Int64
	ReconnectMsSum atomic.Int64
	ReconnectN     atomic.Int64
	BlipGapMsSum   atomic.Int64
	BlipGapBucket  [7]atomic.Int64 // len(blipGapBuckets)+1 (+Inf)
}

// ObserveBlipGap records a blip recovery duration into the histogram.
func (s *DoStats) ObserveBlipGap(ms int64) {
	s.BlipGapMsSum.Add(ms)
	sec := float64(ms) / 1000
	for i, ub := range blipGapBuckets {
		if sec <= ub {
			s.BlipGapBucket[i].Add(1)
			return
		}
	}
	s.BlipGapBucket[len(blipGapBuckets)].Add(1)
}

type Metrics struct {
	site string
	dos  map[string]*DoStats
	keys []string
}

func NewMetrics(site string, cfg *Config) *Metrics {
	m := &Metrics{site: site, dos: map[string]*DoStats{}}
	for _, d := range cfg.doNames() {
		m.dos[d.Name] = &DoStats{Cohort: d.Cohort}
		m.keys = append(m.keys, d.Name)
	}
	sort.Strings(m.keys)
	return m
}

func (m *Metrics) Get(doName string) *DoStats { return m.dos[doName] }

func (m *Metrics) Handler(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "text/plain; version=0.0.4")
	write := func(metric, doName string, s *DoStats, v int64) {
		fmt.Fprintf(w, "%s{site=%q,do=%q,cohort=%q} %d\n", metric, m.site, doName, s.Cohort, v)
	}
	for _, k := range m.keys {
		s := m.dos[k]
		write("do_bench_conns_connected", k, s, s.Connected.Load())
		write("do_bench_ticks_total", k, s, s.Ticks.Load())
		write("do_bench_do_deaths_total", k, s, s.Deaths.Load())
		write("do_bench_path_blips_total", k, s, s.Blips.Load())
		write("do_bench_dial_failures_total", k, s, s.DialFails.Load())
		write("do_bench_reconnect_ms_sum", k, s, s.ReconnectMsSum.Load())
		write("do_bench_reconnect_count", k, s, s.ReconnectN.Load())
		// blip復帰時間ヒストグラム (Prometheus histogram形式・累積バケット)
		cum := int64(0)
		for i, ub := range blipGapBuckets {
			cum += s.BlipGapBucket[i].Load()
			fmt.Fprintf(w, "do_bench_blip_gap_seconds_bucket{site=%q,do=%q,cohort=%q,le=\"%g\"} %d\n", m.site, k, s.Cohort, ub, cum)
		}
		cum += s.BlipGapBucket[len(blipGapBuckets)].Load()
		fmt.Fprintf(w, "do_bench_blip_gap_seconds_bucket{site=%q,do=%q,cohort=%q,le=\"+Inf\"} %d\n", m.site, k, s.Cohort, cum)
		fmt.Fprintf(w, "do_bench_blip_gap_seconds_sum{site=%q,do=%q,cohort=%q} %f\n", m.site, k, s.Cohort, float64(s.BlipGapMsSum.Load())/1000)
		fmt.Fprintf(w, "do_bench_blip_gap_seconds_count{site=%q,do=%q,cohort=%q} %d\n", m.site, k, s.Cohort, cum)
	}
}

// snapshotConnected returns per-DO connected counts for the periodic stats event.
func (m *Metrics) snapshotConnected() map[string]int64 {
	out := make(map[string]int64, len(m.keys))
	for _, k := range m.keys {
		out[k] = m.dos[k].Connected.Load()
	}
	return out
}
