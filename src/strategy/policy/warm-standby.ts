/**
 * flock/ts/policy/warm-standby.ts — warm standby pinger ported from
 * estate-scripts-router's router.ts (startWarmStandby).
 *
 * HFT: dial once, keep alive. Low-frequency liveness pings for LOCAL
 * providers only (no cloud spend — a dead local backend earns circuit
 * strikes here so quarantine engages before user traffic hits it;
 * consecutive successes keep the TCP path warm).
 *
 * The keyed-provider auth fix (2026-09-30): local backends behind a
 * client-key gate (nim-proxy) MUST carry the client key on probes too —
 * an unauthenticated probe 401s, which would trip the circuit despite a
 * valid key. The key is resolved from the provider's keyEnv at probe time
 * (never cached), and a keyed provider with no key configured is SKIPPED,
 * not probed (a 401 strike against a missing key is pure noise).
 *
 * Probe outcomes flow to the injected onProbe callback — wire it to
 * CircuitPolicy.recordProbe (strikes + circuit only, no Elo inflation,
 * no request-DB pollution).
 */

export interface WarmProvider {
  /** Provider name, e.g. "nim-local". */
  name: string;
  /** Base URL, e.g. "http://127.0.0.1:8000/v1". */
  base: string;
  /**
   * Env var holding this provider's client key (e.g. "NIM_PROXY_API_KEY").
   * When set, the key is attached as `Authorization: Bearer <key>` on
   * every probe. When set but empty in the environment, the provider is
   * skipped — never probe a keyed endpoint without the key.
   */
  keyEnv?: string;
}

export interface WarmStandbyOptions {
  /** Ping interval. Default 30000 (30s, estate-scripts parity). */
  intervalMs?: number;
  /** Per-probe deadline. Default 5000. */
  probeTimeoutMs?: number;
  userAgent?: string;
  /**
   * Outcome sink. Wire to CircuitPolicy.recordProbe.
   * (provider, ok, err)
   */
  onProbe?: (provider: string, ok: boolean, err: string) => void;
}

export class WarmStandby {
  private providers: WarmProvider[];
  private intervalMs: number;
  private probeTimeoutMs: number;
  private userAgent: string;
  private onProbe: (provider: string, ok: boolean, err: string) => void;
  private timer: ReturnType<typeof setInterval> | null = null;

  constructor(providers: WarmProvider[], opts: WarmStandbyOptions = {}) {
    this.providers = providers;
    this.intervalMs = opts.intervalMs ?? 30000;
    this.probeTimeoutMs = opts.probeTimeoutMs ?? 5000;
    this.userAgent = opts.userAgent ?? "FlockPolicy/1.0 warm-standby";
    this.onProbe = opts.onProbe ?? (() => {});
  }

  /** Start the interval pinger; fires one immediate tick. */
  start(): void {
    if (this.timer) return;
    this.timer = setInterval(() => {
      this.tick().catch((e) =>
        console.error("[flock-policy] warm standby tick:", String(e).slice(0, 120)),
      );
    }, this.intervalMs);
    this.tick().catch(() => {});
  }

  stop(): void {
    if (this.timer) {
      clearInterval(this.timer);
      this.timer = null;
    }
  }

  get running(): boolean {
    return this.timer !== null;
  }

  /** One full ping sweep. Public so tests can drive it without timers. */
  async tick(): Promise<void> {
    for (const p of this.providers) {
      await this.probeOne(p);
    }
  }

  private async probeOne(p: WarmProvider): Promise<void> {
    const headers: Record<string, string> = {
      Accept: "application/json",
      "User-Agent": this.userAgent,
    };
    // Keyed local backends need the client key on probes too. Resolve at
    // probe time (never cache) so key rotations land without a restart.
    // A keyed provider with no key is skipped, never probed.
    if (p.keyEnv) {
      const keyVal = process.env[p.keyEnv] || "";
      if (!keyVal) return;
      headers["Authorization"] = `Bearer ${keyVal}`;
    }
    try {
      const r = await fetch(`${p.base.replace(/\/+$/, "")}/models`, {
        headers,
        signal: AbortSignal.timeout(this.probeTimeoutMs),
      });
      this.onProbe(p.name, r.ok, r.ok ? "" : `warm-standby http_${r.status}`);
    } catch (e) {
      this.onProbe(p.name, false, `warm-standby: ${String(e).slice(0, 120)}`);
    }
  }
}
