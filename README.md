# do-bench

Cloudflare Durable Objects (DO) の実効SLA検証ハーネス。

ライブ配信のコメントfanoutにDOを採用する前提で、**「アクティブに使われているDOが、自分のデプロイ以外の要因（Cloudflareのランタイム更新・ホスト移行等）でどれくらいの頻度で死ぬか」「死んでから復旧までどれくらいかかるか」** を実測する。

設計の背景・判定ロジック・コスト見積もりの詳細は [docs/design.md](docs/design.md) を参照。

## 構成

```
worker/   Cloudflare Worker + BenchDO (観測対象のDO)
prober/   Go製の観測クライアント (VPSで常駐させる)
```

- **BenchDO**: 起動時に世代ID (UUID) を生成し、接続中の全WSクライアントへ1秒ごとにtick（世代ID・seq・colo入り）をブロードキャストする。世代の履歴はDO内のSQLiteにも記録。
- **Prober**: 各DOにWSを張り続け、tickの途絶（3.5秒）を検知したら即再接続して世代IDを照合する。
  - 世代IDが変わっていた → **DO死** (`do_death`)。uptime・downtime・巻き込まれ接続数を記録
  - 世代IDが同じ → **経路の問題** (`path_blip`)。DOは生きていた（誤検知として分離）
- 一次データは `data/events-YYYY-MM-DD.jsonl`。Grafana等はこの上の可視化レイヤーにすぎない。

## セットアップ

### 1. Worker のデプロイ

```sh
cd worker
pnpm install
npx wrangler secret put KILL_TOKEN   # 実験B(意図的kill)用の任意トークン
pnpm deploy
```

エンドポイント（DO名は `s-0`〜 / `m-0`〜 / `l-0`〜 のパターンのみ受付）:

| パス | 用途 |
|---|---|
| `GET /ws/:name` | WebSocket接続（tick配信） |
| `GET /info/:name` | 現在の世代・接続数などのスナップショット |
| `GET /history/:name` | SQLiteに残る世代（再起動）履歴 |
| `POST /kill/:name` | `Authorization: Bearer $KILL_TOKEN` で意図的にDOを殺す（実験B用） |

### 2. Prober の起動（VPS上）

```sh
cd prober
go build -o prober .
cp config.example.json config.json   # workerUrl / site を編集
ulimit -n 65536                       # L コホート(5000本×3)ぶんのfdを確保
./prober -config config.json
```

- `site` はVPSごとに変える（例: `vultr-tokyo` / `sakura-ishikari`）。2拠点から観測すると、切断の同時性で経路問題とDO死の切り分けが強くなる。
- `healthcheckUrl` に healthchecks.io のURLを入れると1分ごとにpingを打つ。**Prober自身が落ちていた期間は測定の穴になる**ので必ず設定する。
- メトリクスは `http://127.0.0.1:9209/metrics` (Prometheusテキスト形式)。Grafana Cloudに送る場合はGrafana Alloyでscrape→remote_writeする。

systemd unit の例:

```ini
[Unit]
Description=do-bench prober
After=network-online.target

[Service]
ExecStart=/opt/do-bench/prober -config /opt/do-bench/config.json
WorkingDirectory=/opt/do-bench
Restart=always
RestartSec=5
LimitNOFILE=65536

[Install]
WantedBy=multi-user.target
```

### 3. 集計

```sh
./prober report -dir ./data           # markdownでコホート別サマリ・時間帯ヒートマップを出力
```

## 測定中の注意

- **測定期間中は worker をデプロイしない**。デプロイは全DOを再起動させるため、運営都合の死と区別がつかなくなる。やむを得ずデプロイした場合は日時を記録して集計から除外する。
- コホート構成（デフォルト: s=10DO×1本 / m=4DO×500本 / l=3DO×5000本）は `config.json` の `cohorts` で変更できる。DO側は接続が来た名前で自動生成されるので、Worker側の変更は不要。
