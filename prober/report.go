package main

import (
	"bufio"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

// runReport reads events-*.jsonl and prints a markdown summary.
// SLAの主指標:
//   - 世代uptime (oldGenBootAt -> firstLostAt): 「1つのDOが死なずに生きる時間」の実測分布
//   - downtime (firstLostAt -> recoveredAt): 死んでから復旧までの時間
func runReport(args []string) {
	fs := flag.NewFlagSet("report", flag.ExitOnError)
	dir := fs.String("dir", "./data", "directory containing events-*.jsonl")
	tzName := fs.String("tz", "Asia/Tokyo", "timezone for hourly histogram")
	fs.Parse(args)

	loc, err := time.LoadLocation(*tzName)
	if err != nil {
		loc = time.UTC
	}

	files, _ := filepath.Glob(filepath.Join(*dir, "events-*.jsonl"))
	if len(files) == 0 {
		fmt.Fprintf(os.Stderr, "no events-*.jsonl found in %s\n", *dir)
		os.Exit(1)
	}
	sort.Strings(files)

	type death struct {
		ts         time.Time
		do, cohort string
		oldColo    string
		newColo    string
		uptimeMs   float64
		downtimeMs float64
		connsLost  float64
	}
	var deaths []death
	blipsByCohort := map[string]int{}
	dosSeen := map[string]map[string]bool{} // cohort -> set of DO names
	var firstTs, lastTs time.Time
	sites := map[string]bool{}

	for _, path := range files {
		f, err := os.Open(path)
		if err != nil {
			continue
		}
		sc := bufio.NewScanner(f)
		sc.Buffer(make([]byte, 1024*1024), 1024*1024)
		for sc.Scan() {
			var ev map[string]any
			if json.Unmarshal(sc.Bytes(), &ev) != nil {
				continue
			}
			ts, _ := time.Parse(time.RFC3339Nano, str(ev, "ts"))
			if !ts.IsZero() {
				if firstTs.IsZero() || ts.Before(firstTs) {
					firstTs = ts
				}
				if ts.After(lastTs) {
					lastTs = ts
				}
			}
			if s := str(ev, "site"); s != "" {
				sites[s] = true
			}
			cohort := str(ev, "cohort")
			switch str(ev, "type") {
			case "gen_first_seen", "do_death":
				if dosSeen[cohort] == nil {
					dosSeen[cohort] = map[string]bool{}
				}
				dosSeen[cohort][str(ev, "do")] = true
			}
			switch str(ev, "type") {
			case "do_death":
				deaths = append(deaths, death{
					ts: ts, do: str(ev, "do"), cohort: cohort,
					oldColo: str(ev, "oldColo"), newColo: str(ev, "newColo"),
					uptimeMs: num(ev, "uptimeMs"), downtimeMs: num(ev, "downtimeMs"),
					connsLost: num(ev, "connsLost"),
				})
			case "path_blip":
				blipsByCohort[cohort]++
			}
		}
		f.Close()
	}

	fmt.Printf("# do-bench report\n\n")
	fmt.Printf("- 期間: %s 〜 %s (%.1f 日)\n",
		firstTs.In(loc).Format("2006-01-02 15:04"), lastTs.In(loc).Format("2006-01-02 15:04"),
		lastTs.Sub(firstTs).Hours()/24)
	fmt.Printf("- 観測サイト: %s\n", strings.Join(keys(sites), ", "))
	fmt.Printf("- DO死イベント合計: %d\n\n", len(deaths))

	fmt.Printf("## コホート別サマリ\n\n")
	fmt.Printf("| cohort | DOs | deaths | 平均uptime | 中央値uptime | p95 uptime | downtime p50 | downtime p95 | downtime max | blips |\n")
	fmt.Printf("|---|---|---|---|---|---|---|---|---|---|\n")
	cohorts := keys(dosSeen)
	for _, co := range cohorts {
		var up, down []float64
		for _, d := range deaths {
			if d.cohort == co {
				up = append(up, d.uptimeMs)
				down = append(down, d.downtimeMs)
			}
		}
		fmt.Printf("| %s | %d | %d | %s | %s | %s | %s | %s | %s | %d |\n",
			co, len(dosSeen[co]), len(up),
			durStr(mean(up)), durStr(pct(up, 50)), durStr(pct(up, 95)),
			durStr(pct(down, 50)), durStr(pct(down, 95)), durStr(pct(down, 100)),
			blipsByCohort[co])
	}

	fmt.Printf("\n## 時間帯別の死亡分布 (%s)\n\n", loc)
	hourly := make([]int, 24)
	for _, d := range deaths {
		hourly[d.ts.In(loc).Hour()]++
	}
	max := 1
	for _, n := range hourly {
		if n > max {
			max = n
		}
	}
	for h := 0; h < 24; h++ {
		bar := strings.Repeat("█", hourly[h]*40/max)
		fmt.Printf("%02d時 | %-40s %d\n", h, bar, hourly[h])
	}

	fmt.Printf("\n## 直近の死亡イベント (最大50件)\n\n")
	fmt.Printf("| 時刻 (%s) | DO | uptime | downtime | connsLost | colo |\n", loc)
	fmt.Printf("|---|---|---|---|---|---|\n")
	start := 0
	if len(deaths) > 50 {
		start = len(deaths) - 50
	}
	for _, d := range deaths[start:] {
		colo := d.oldColo
		if d.newColo != d.oldColo {
			colo = d.oldColo + "→" + d.newColo
		}
		fmt.Printf("| %s | %s | %s | %s | %.0f | %s |\n",
			d.ts.In(loc).Format("01-02 15:04:05"), d.do,
			durStr(d.uptimeMs), durStr(d.downtimeMs), d.connsLost, colo)
	}
}

func str(ev map[string]any, k string) string {
	s, _ := ev[k].(string)
	return s
}

func num(ev map[string]any, k string) float64 {
	f, _ := ev[k].(float64)
	return f
}

func keys[V any](m map[string]V) []string {
	var out []string
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

func mean(xs []float64) float64 {
	if len(xs) == 0 {
		return 0
	}
	sum := 0.0
	for _, x := range xs {
		sum += x
	}
	return sum / float64(len(xs))
}

func pct(xs []float64, p int) float64 {
	if len(xs) == 0 {
		return 0
	}
	sorted := append([]float64(nil), xs...)
	sort.Float64s(sorted)
	idx := (len(sorted) - 1) * p / 100
	return sorted[idx]
}

func durStr(ms float64) string {
	if ms <= 0 {
		return "-"
	}
	d := time.Duration(ms) * time.Millisecond
	switch {
	case d < time.Minute:
		return fmt.Sprintf("%.1fs", d.Seconds())
	case d < time.Hour:
		return fmt.Sprintf("%.1fm", d.Minutes())
	default:
		return fmt.Sprintf("%.1fh", d.Hours())
	}
}
