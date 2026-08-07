#!/usr/bin/env python3
"""spotwatch: CFメンテ波を検知したらフレッシュDO(s-90..s-92)を3時間だけ観測する.

仮説検証: 「DO死は稼働時間起点のローリング更新で起きる。生まれたてのDOは
波の最中でも死ににくい」。波の最中にフレッシュDOを投入し、2時間/3時間の
生存率を本隊のSLA(2時間で約17.5%被弾)と比較する。

トリガー: 本隊JSONLで直近30分にdo_deathが2件以上 → スポット観測開始
クールダウン: 前回開始から6時間(s-90..92がアイドル退避してフレッシュに戻るのを待つ)
"""
import json, os, subprocess, sys, time
from datetime import datetime, timezone, timedelta

DATA_DIR = "/home/ubuntu/do-bench/data"
SPOT_DIR = "/home/ubuntu/do-bench/data-spot"
PROBER = "/home/ubuntu/do-bench/prober-spot"
CONFIG = "/home/ubuntu/do-bench/config-spot.json"
RUNS_LOG = os.path.join(SPOT_DIR, "runs.jsonl")
TRIGGER_DEATHS = 2
TRIGGER_WINDOW_S = 1800
COOLDOWN_S = 6 * 3600
RUN_SECONDS = 3 * 3600
POLL_S = 60


def recent_deaths():
    now = datetime.now(timezone.utc)
    cutoff = now - timedelta(seconds=TRIGGER_WINDOW_S)
    out = []
    for d in (now - timedelta(days=1), now):
        path = os.path.join(DATA_DIR, "events-%s.jsonl" % d.strftime("%Y-%m-%d"))
        if not os.path.exists(path):
            continue
        with open(path) as f:
            for line in f:
                if '"type":"do_death"' not in line:
                    continue
                try:
                    e = json.loads(line)
                    ts = datetime.fromisoformat(e["ts"].replace("Z", "+00:00"))
                except Exception:
                    continue
                if ts >= cutoff:
                    out.append((e["ts"], e.get("do")))
    return out


def last_run_start():
    if not os.path.exists(RUNS_LOG):
        return None
    last = None
    with open(RUNS_LOG) as f:
        for line in f:
            try:
                last = json.loads(line)
            except Exception:
                pass
    if last is None:
        return None
    return datetime.fromisoformat(last["startedAt"].replace("Z", "+00:00"))


def log_run(rec):
    os.makedirs(SPOT_DIR, exist_ok=True)
    with open(RUNS_LOG, "a") as f:
        f.write(json.dumps(rec) + "\n")
        f.flush()
        os.fsync(f.fileno())


def main():
    print("spotwatch started", flush=True)
    while True:
        try:
            deaths = recent_deaths()
            if len(deaths) >= TRIGGER_DEATHS:
                prev = last_run_start()
                now = datetime.now(timezone.utc)
                if prev is None or (now - prev).total_seconds() >= COOLDOWN_S:
                    rec = {
                        "startedAt": now.strftime("%Y-%m-%dT%H:%M:%SZ"),
                        "trigger": deaths[-5:],
                        "runSeconds": RUN_SECONDS,
                    }
                    log_run(rec)
                    print("wave detected (%d deaths/30min) -> spot run start" % len(deaths), flush=True)
                    # timeoutで3時間後に確実に終了。終了後は接続が消え、
                    # スポットDOはアイドル退避して次回はフレッシュ起動になる。
                    r = subprocess.run(
                        ["timeout", str(RUN_SECONDS), PROBER, "-config", CONFIG],
                        stdout=subprocess.DEVNULL, stderr=subprocess.STDOUT)
                    print("spot run finished rc=%d" % r.returncode, flush=True)
        except Exception as ex:
            print("spotwatch error: %r" % ex, flush=True)
        time.sleep(POLL_S)


if __name__ == "__main__":
    main()
