import { BenchDO, TickerDO, type Env } from "./bench-do";

export { BenchDO, TickerDO };

// コホート命名: s-0..s-9 / m-0..m-3 / l-0..l-2。
// パターン外の名前を弾いて、タイポで野良DOが増殖するのを防ぐ。
const NAME_RE = /^[sml]-\d{1,3}$/;

export default {
  async fetch(request, env): Promise<Response> {
    const url = new URL(request.url);

    // Ticker (集計DO役) の管理エンドポイント。シングルトン。要認証。
    const tm = url.pathname.match(/^\/ticker\/(targets|stop|info)$/);
    if (tm) {
      if (tm[1] !== "info") {
        const auth = request.headers.get("authorization") ?? "";
        if (!env.KILL_TOKEN || auth !== `Bearer ${env.KILL_TOKEN}`) {
          return new Response("forbidden", { status: 403 });
        }
      }
      const stub = env.TICKER_DO.get(env.TICKER_DO.idFromName("ticker"), {
        locationHint: "apac",
      });
      return stub.fetch(new Request(`https://do/${tm[1]}`, request));
    }

    const m = url.pathname.match(/^\/(ws|info|history|kill)\/([^/]+)$/);
    if (!m) {
      return new Response(
        "do-bench: /ws/:name /info/:name /history/:name /kill/:name /ticker/{targets,stop,info}",
        { status: url.pathname === "/" ? 200 : 404 }
      );
    }
    const [, action, name] = m;
    if (!NAME_RE.test(name)) {
      return new Response("invalid DO name", { status: 400 });
    }
    if (action === "kill") {
      const auth = request.headers.get("authorization") ?? "";
      if (!env.KILL_TOKEN || auth !== `Bearer ${env.KILL_TOKEN}`) {
        return new Response("forbidden", { status: 403 });
      }
    }

    const id = env.BENCH_DO.idFromName(name);
    // 国内限定サービスの想定に合わせ、DO をアジア圏に置く。
    // 実際の配置 colo は DO 自身が trace で自己申告する。
    const stub = env.BENCH_DO.get(id, { locationHint: "apac" });

    const forwarded = new Request(`https://do/${action}`, request);
    forwarded.headers.set("x-edge-colo", String(request.cf?.colo ?? ""));
    return stub.fetch(forwarded);
  },
} satisfies ExportedHandler<Env>;
