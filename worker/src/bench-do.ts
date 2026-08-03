import { DurableObject } from "cloudflare:workers";

export interface Env {
  BENCH_DO: DurableObjectNamespace<BenchDO>;
  TICKER_DO: DurableObjectNamespace<TickerDO>;
  KILL_TOKEN?: string;
}

// hello / tick は同一スキーマ。Prober は最初に受信したメッセージを
// そのまま世代識別に使えるため、メッセージ順序に依存しない。
interface WireMsg {
  t: "hello" | "tick";
  gen: string;
  seq: number;
  bootAt: number;
  colo: string;
  edgeColo?: string;
  conns: number;
  now: number;
}

export class BenchDO extends DurableObject<Env> {
  private gen = crypto.randomUUID();
  private bootAt = Date.now();
  private colo = "";
  private seq = 0;
  private sockets = new Set<WebSocket>();
  private coloPromise: Promise<void> | null = null;

  constructor(ctx: DurableObjectState, env: Env) {
    super(ctx, env);
    // 世代の履歴を SQLite に残す。コンストラクタ実行 = プロセス起動なので
    // この INSERT が「再起動の記録」そのものになる。
    ctx.storage.sql.exec(
      "CREATE TABLE IF NOT EXISTS boots (gen TEXT PRIMARY KEY, boot_at INTEGER NOT NULL, colo TEXT NOT NULL DEFAULT '')"
    );
    ctx.storage.sql.exec(
      "INSERT INTO boots (gen, boot_at) VALUES (?, ?)",
      this.gen,
      this.bootAt
    );
  }

  // Ticker DO から毎秒 RPC で呼ばれる配信メソッド。本番の
  // 「集計DO → シャードDO への RPC → 全視聴者へ fanout」を再現する。
  // 送信ループはチャンク化し、チャンク間でイベントループに制御を返して
  // 受信イベント(接続/切断)が滞留しないようにする。
  async broadcastTick(): Promise<{ sent: number; ms: number }> {
    const t0 = Date.now();
    this.coloPromise ??= this.resolveColo();
    this.seq++;
    const payload = JSON.stringify(this.snapshot("tick"));
    let sent = 0;
    let i = 0;
    for (const ws of this.sockets) {
      try {
        ws.send(payload);
        sent++;
      } catch {
        this.sockets.delete(ws);
      }
      if (++i % 500 === 0) {
        await new Promise((r) => setTimeout(r, 0));
      }
    }
    return { sent, ms: Date.now() - t0 };
  }

  async fetch(request: Request): Promise<Response> {
    // colo 解決はコンストラクタでは I/O できないため、最初のイベントで開始する。
    // 起動レイテンシ（= 復旧時間の測定値）を汚さないよう待たない。
    this.coloPromise ??= this.resolveColo();

    const path = new URL(request.url).pathname;
    switch (path) {
      case "/ws":
        return this.handleWs(request);
      case "/info":
        return Response.json(this.snapshot("hello"));
      case "/history":
        return Response.json(
          this.ctx.storage.sql
            .exec("SELECT gen, boot_at, colo FROM boots ORDER BY boot_at DESC LIMIT 200")
            .toArray()
        );
      case "/kill":
        // 実験B（意図的に殺して再接続ストームを観測）用。abort() は戻らないので
        // レスポンスを返してから非同期で死ぬ。
        setTimeout(() => this.ctx.abort(), 100);
        return Response.json({ ok: true, gen: this.gen });
      default:
        return new Response("not found", { status: 404 });
    }
  }

  private handleWs(request: Request): Response {
    if (request.headers.get("upgrade")?.toLowerCase() !== "websocket") {
      return new Response("expected websocket", { status: 426 });
    }
    const pair = new WebSocketPair();
    const [client, server] = Object.values(pair);
    // Hibernation API は意図的に使わない。休眠させず「アクティブなDOが
    // 外部要因で死ぬ頻度」だけを測るため、接続中は常時メモリ常駐にする。
    server.accept();
    this.sockets.add(server);
    const drop = () => this.sockets.delete(server);
    server.addEventListener("close", drop);
    server.addEventListener("error", drop);

    server.send(
      JSON.stringify({
        ...this.snapshot("hello"),
        edgeColo: request.headers.get("x-edge-colo") ?? "",
      })
    );
    return new Response(null, { status: 101, webSocket: client });
  }

  private snapshot(t: WireMsg["t"]): WireMsg {
    return {
      t,
      gen: this.gen,
      seq: this.seq,
      bootAt: this.bootAt,
      colo: this.colo,
      conns: this.sockets.size,
      now: Date.now(),
    };
  }

  private async resolveColo(): Promise<void> {
    try {
      // DO 内からの subrequest は DO の配置場所から出るため、
      // trace の colo が DO 自身の配置 PoP を示す。
      const res = await fetch("https://cloudflare.com/cdn-cgi/trace");
      const m = (await res.text()).match(/^colo=(\S+)$/m);
      if (m) {
        this.colo = m[1];
        this.ctx.storage.sql.exec(
          "UPDATE boots SET colo = ? WHERE gen = ?",
          this.colo,
          this.gen
        );
      }
    } catch {
      // colo 不明のまま続行。tick には空文字が載る。
    }
  }
}

// TickerDO: 本番の「集計DO」役。1秒アラームで全シャードDOへ RPC 配信する。
// 前の配信ラウンドが終わるまで次を始めない(アラーム再設定はラウンド完了後)ため、
// シャードが遅い場合は自然に間引かれる — 本番の集計側と同じバックプレッシャー。
export class TickerDO extends DurableObject<Env> {
  private lastRound: { at: number; ms: number; results: Record<string, number | string> } | null =
    null;
  private rounds = 0;

  constructor(ctx: DurableObjectState, env: Env) {
    super(ctx, env);
    ctx.storage.sql.exec(
      "CREATE TABLE IF NOT EXISTS config (k TEXT PRIMARY KEY, v TEXT NOT NULL)"
    );
  }

  async fetch(request: Request): Promise<Response> {
    const path = new URL(request.url).pathname;
    switch (path) {
      case "/targets": {
        // POST: {"targets": ["s-0", ...]} を保存し、アラーム駆動を開始する
        const body = (await request.json()) as { targets: string[] };
        this.ctx.storage.sql.exec(
          "INSERT INTO config (k, v) VALUES ('targets', ?) ON CONFLICT(k) DO UPDATE SET v = excluded.v",
          JSON.stringify(body.targets)
        );
        await this.ctx.storage.setAlarm(Date.now() + 1000);
        return Response.json({ ok: true, targets: body.targets });
      }
      case "/stop":
        await this.ctx.storage.deleteAlarm();
        return Response.json({ ok: true, stopped: true });
      case "/info":
        return Response.json({
          rounds: this.rounds,
          lastRound: this.lastRound,
          alarm: await this.ctx.storage.getAlarm(),
        });
      default:
        return new Response("not found", { status: 404 });
    }
  }

  async alarm(): Promise<void> {
    const row = this.ctx.storage.sql
      .exec("SELECT v FROM config WHERE k = 'targets'")
      .toArray()[0];
    if (!row) return; // 設定なし: アラーム停止
    const targets = JSON.parse(row.v as string) as string[];

    const t0 = Date.now();
    const results: Record<string, number | string> = {};
    await Promise.all(
      targets.map(async (name) => {
        try {
          const stub = this.env.BENCH_DO.get(this.env.BENCH_DO.idFromName(name), {
            locationHint: "apac",
          });
          const r = await stub.broadcastTick();
          results[name] = r.sent;
        } catch (e) {
          results[name] = `ERR: ${String(e).slice(0, 80)}`;
        }
      })
    );
    this.rounds++;
    this.lastRound = { at: t0, ms: Date.now() - t0, results };

    // ラウンド完了後に次のアラームを設定(1秒スロットに整列)。
    // ラウンドが1秒を超えた場合は次スロットへ自然にスキップされる。
    const next = t0 + 1000 - ((Date.now() - t0) % 1000);
    await this.ctx.storage.setAlarm(Math.max(next, Date.now() + 100));
  }
}
