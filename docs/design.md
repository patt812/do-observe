# do-bench 設計ドキュメント

作成: 2026-08-01

## 1. 背景と目的

ライブ配信のコメントfanoutにCloudflare Durable Objects (DO) を採用する検討をしている。別途のベンチマークで **1シャード5000接続が安全圏** と確認済み。コメントは1秒ごとに集計してブロードキャストする方式で、接続時にDO内のSQLiteから直近N件を引くため、数tick程度の欠損は再接続時に復旧できる。

懸念は以下の2点で、いずれも公式には数値が公開されていない：

1. **DOが運営都合（Cloudflare側）で死ぬ頻度** — 公式ドキュメントは「occasionally shut down」としか言っておらず、個別DOの稼働率に対するSLAは存在しない
2. **死んでから復旧までの時間** — 5000接続の一斉再接続は負荷で死に続けることを別途観測済みのため、素の復旧時間と再接続戦略の両方が要検証

### 公式ドキュメントで確認済みの事実

- DOの再起動要因: 自分のWorkerデプロイ / Cloudflareのランタイム更新 / ホスト移行 / ネットワーク分断
  ([Lifecycle](https://developers.cloudflare.com/durable-objects/concepts/durable-object-lifecycle/), [Known issues](https://developers.cloudflare.com/durable-objects/platform/known-issues/))
- **シャットダウン時、WebSocketは自動的に切断される**。Hibernation APIは休眠をまたいで接続を保持するが、再起動をまたいでは保持しない
- workerd はほぼ毎日リリースされており ([releases](https://github.com/cloudflare/workerd/releases))、本番ロールアウトの頻度は非公開
- 社内の事前観測: JST深夜帯にWS closeが多発（closeフレームなしの無言死も含む）。JST深夜＝米国営業時間であり、ランタイム更新のロールアウト時間帯という仮説と整合する。ただしこの観測では「DO死」と「経路のchurn」が未分離 → 本ハーネスで白黒つける

## 2. 測定設計

### 実験A: 死亡頻度の長期観測（本ハーネス）

3コホート・計17DOを日本（locationHint: apac）に配置し、2〜4週間の連続観測を行う。

| コホート | DO数 | 接続数/DO | 目的 |
|---|---|---|---|
| s | 10 | 1 | ほぼ無負荷DOのベースライン。サンプル数の主力 |
| m | 4 | 500 | 接続数と死亡率の関係（線形か閾値型か） |
| l | 3 | 5000 | 本番安全圏の実負荷での実測値 |

接続数と死亡率の因果は未知（多いとリソース圧で死にやすい説 / 少ないと移行対象に選ばれやすい説の両方がありうる）ため、コホート間比較で検証する。

### 検出方式: 世代ID + 1秒tick

- DOはコンストラクタで世代ID (UUID) を生成。**コンストラクタ実行＝プロセス起動**なので、世代IDの変化が再起動の決定的証拠になる
- DOは1秒ごとに全接続へtick（世代ID・seq・bootAt・colo・時刻）をブロードキャスト。本番のコメント集計配信と同じ負荷プロファイルであり、キープアライブを兼ねる
- Proberはtickが3.5秒途絶えたら接続喪失とみなし（**closeフレームに依存しない**。無言死はタイムアウトで検出）、即座に再接続して世代IDを照合:
  - **世代IDが変化** → DO死。`do_death` イベント（モニタ単位で新世代ごとに1回だけ発行）
  - **世代IDが同じ** → DOは生きていた。`path_blip` として分離（経路障害の誤検知除去）
- 個別WSの切断は本番でも織り込み済みのため無視する。**SLAの単位は「1つのDOが死なずに生きる時間」（世代uptime）**

### 主要メトリクス

| 指標 | 定義 |
|---|---|
| 世代uptime | `oldGenBootAt → firstLostAt`。MTBF（平均再起動間隔）の実測分布 |
| downtime | `firstLostAt → recoveredAt`（新世代の最初のメッセージ受信）。復旧時間 |
| 時間帯分布 | 死亡イベントのJST時間帯ヒストグラム。ランタイム更新ロールアウト仮説の検証 |
| colo変化 | 死亡前後の配置PoP。ホスト移行 vs その場再起動の推定 |
| 同時死相関 | 複数DOが同時刻に死んだか。ランタイム更新の波の検出 |

### ノイズ対策

- 測定期間中は自分のWorkerデプロイを凍結（デプロイ＝全DO再起動のため）
- Prober切断中のアイドルeviction（70〜140秒）を強制再起動と誤認しないよう、再接続はジッター付き最大1.5秒で即時実施。Prober自体の死活は healthchecks.io で監視し、停止期間は集計から除外
- 2拠点（海外系VPS＋国内系VPS）から観測し、切断の同時性で経路問題を切り分け

### 実験B: 復旧・再接続ストーム（別途、使い捨てDO）

自然死はレアイベントでサンプルが貯まらないため、復旧特性は `POST /kill/:name`（`ctx.abort()`）で**意図的に殺して**測る:

- 5000接続のDOを殺し、全クライアント再接続 → 全員復帰までの時間を計測
- 再接続戦略のA/B: 即時一斉 / exponential backoff / full jitter / 時間分散ウィンドウ
- 目標は「1秒で全員復帰」ではなく「コメントtickが1〜2回落ちるだけで、seq番号による追いつきで欠損なく復旧」
- `abort()` とランタイム更新による死が同挙動かは保証がないため、実験Aで捕まえた自然死の復旧時間と突き合わせる

## 3. 技術上の設計判断

- **Hibernation APIを使わない（標準WebSocket API）**: 休眠させず常時メモリ常駐にすることで、「アクティブなDOが外部要因で死ぬ頻度」だけを純粋に測る。本番の配信中も常時メッセージが流れるので休眠しない前提と一致
- **tick配信はoutgoing**: DOのWebSocket課金はincomingのみ（20:1）・outgoingは無料のため、5000本×毎秒配信でもリクエスト費ゼロ。Proberは基本沈黙（tick受信のみ）
- **colo自己申告**: DO内から `cloudflare.com/cdn-cgi/trace` をfetchすると、subrequestがDOの配置場所から出るため自身のcoloが取れる。起動レイテンシを汚さないよう非同期で解決
- **一次データはローカルJSONL**: Grafana/Datadog等の可視化レイヤーが何であれ、JSONLさえあれば後から再集計・別ツールへの流し込みが可能

## 4. コスト見積もり（2026-08-01 に公式ページで裏取り済み）

| 項目 | 計算 | 月額 |
|---|---|---|
| Workers Paid 基本料 | — | $5.00 |
| DO duration | 17 DO × 331,776 GB-s − 無料枠40万GB-s ≈ 524万GB-s × $12.50/M | 約$65.50 |
| DO リクエスト | tick配信はoutgoing無料。接続確立＋再接続のみで無料枠100万req内 | $0 |
| SQLite | 世代履歴のみ。無料枠の誤差レベル | $0 |
| VPS 2拠点 | Vultr東京 1GB $5〜6 + さくら/ConoHa ¥460〜1,065 | 約¥1,100〜2,000 |
| Grafana Cloud / healthchecks.io | 無料枠 | ¥0 |

**合計: 月約$72〜80（約11,000〜12,500円）**。durationはDOが「メモリに載っている時間」だけで決まり接続数に依存しないため、コストレバーはDO数のみ（1DO≈$4.15/月）。

出典: [DO Pricing](https://developers.cloudflare.com/durable-objects/platform/pricing/) / [Workers Pricing](https://developers.cloudflare.com/workers/platform/pricing/)

## 5. イベントスキーマ（JSONL）

全イベント共通: `ts` (UTC, RFC3339Nano) / `type` / `site`

| type | 主なフィールド |
|---|---|
| `prober_start` / `prober_stop` | `workerUrl`, `totalConns`, `cohorts` |
| `gen_first_seen` | `do`, `cohort`, `gen`, `bootAt`, `colo`, `edgeColo` |
| `do_death` | `do`, `cohort`, `oldGen`, `newGen`, `oldColo`, `newColo`, `oldGenBootAt`, `firstLostAt`, `recoveredAt`, `uptimeMs`, `downtimeMs`, `connsLost` |
| `path_blip` | `do`, `cohort`, `conn`, `gen`, `gapMs`, `reason` |
| `stats` (5分ごと) | `connected` (DO別接続数) |
