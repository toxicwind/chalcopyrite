/**
 * flock/ts/policy/circuit.ts — quarantine + circuit policy ported from
 * estate-scripts-router's router_matrix.ts (Matrix.record / recordProbe /
 * circuitOk / laneDead / noteEntitlement404 / flap tracker /
 * startQuarantineProber).
 *
 * Boundary with the Rust proxy: proxy/src/circuit.rs owns the per-request
 * in-proxy breaker (AstMatrix semantics: 5 failures, fixed 30s timeout, 2
 * half-open successes to close). This module is the POLICY tier above it —
 * the things circuit.rs deliberately does not do:
 *
 *   - failure-class-aware counting (401/402 dead-key counts 3x; 404 benches
 *     the MODEL not the provider; 429s never open the circuit)
 *   - exponential-backoff quarantine levels (BASE * 2^(level-1), capped)
 *   - active re-probing: an expired backoff earns ONE probe, success
 *     half-opens the provider instead of waiting for live traffic
 *   - recordProbe: synthetic liveness pings move strikes/circuits WITHOUT
 *     touching Elo or request analytics (no Elo inflation, no DB pollution)
 *   - laneDead: dead-lane exclusion for race sets — persistent 429s, shims
 *     whose /models 200s while completions 504, hard 401/402s between
 *     circuit openings. 6+ consecutive failures, no success, within 10 min.
 *   - entitlement-404 permanent bench: one 404 on a model id benches it
 *     for the process lifetime (zero retry burn); re-admission when the
 *     catalog re-lists the id
 *   - flap tracker: models returning empty (non-substantive) completions
 *     collect strikes; 3 strikes within 10 min benches the model until decay
 *
 * Healing events flow through the injected onHealing callback — wire it to
 * the PolicyHealthDB.recordHealing in health.ts or to the Rust proxy's
 * observation feed.
 */

export type CircuitState = "closed" | "open" | "half";

/** Defaults from estate-scripts router_config.ts (env-overridable there). */
export const QUARANTINE_BASE_S = 60;
export const QUARANTINE_MAX_S = 1800;
export const QUARANTINE_PROBE_MS = 15000;
/** Consecutive weighted failures that open the circuit. */
export const CIRCUIT_OPEN_THRESHOLD = 3;
/** laneDead: failures with no success inside this window exclude the lane. */
export const LANEDEAD_MIN_FAILS = 6;
export const LANEDEAD_WINDOW_S = 600;
/** Flap tracker: strikes within the window that bench a model. */
export const FLAP_STRIKES = 3;
export const FLAP_WINDOW_S = 600;

export interface HealingEvent {
  provider: string;
  model: string;
  event: string;
  prev: string;
  next: string;
  details?: string;
}

export interface CircuitInfo {
  state: CircuitState;
  quarantine_level: number;
  consecutive_failures: number;
  backoff_s: number;
  open_until: number | null;
  probe_in_s: number | null;
  last_error: { status: number; err: string; at: number } | null;
}

export interface CircuitPolicyOptions {
  /** Called on every circuit/quarantine transition. */
  onHealing?: (e: HealingEvent) => void;
  /**
   * Re-admission check for entitlement-benched models: return true when the
   * provider's catalog serves the model id again (clears the bench).
   * Defaults to "never re-admitted" (bench sticks for process lifetime).
   */
  catalogServes?: (provider: string, model: string) => boolean;
  quarantineBaseS?: number;
  quarantineMaxS?: number;
}

export class CircuitPolicy {
  private state = new Map<string, CircuitState>();
  private openUntil = new Map<string, number>();
  private quarantineLevel = new Map<string, number>();
  private fails = new Map<string, [count: number, at: number]>();
  private lastError = new Map<string, { status: number; err: string; at: number }>();
  /** entitlementDead: "provider:model" -> benched_at (process lifetime). */
  private entitlementDead = new Map<string, number>();
  /** Flap tracker: "provider/model" -> { strikes, last }. */
  private emptyStrikes = new Map<string, { n: number; last: number }>();
  private opts: Required<Pick<CircuitPolicyOptions, "catalogServes">> &
    Pick<CircuitPolicyOptions, "onHealing"> & {
      quarantineBaseS: number;
      quarantineMaxS: number;
    };

  constructor(opts: CircuitPolicyOptions = {}) {
    this.opts = {
      onHealing: opts.onHealing,
      catalogServes: opts.catalogServes ?? (() => false),
      quarantineBaseS: opts.quarantineBaseS ?? QUARANTINE_BASE_S,
      quarantineMaxS: opts.quarantineMaxS ?? QUARANTINE_MAX_S,
    };
  }

  /** Register a provider so it starts closed. Idempotent. */
  register(provider: string): void {
    if (!this.state.has(provider)) this.state.set(provider, "closed");
  }

  private heal(e: HealingEvent): void {
    try {
      this.opts.onHealing?.(e);
    } catch {
      /* healing feed is observability; never break routing */
    }
  }

  private backoff(level: number): number {
    return Math.min(
      this.opts.quarantineBaseS * 2 ** (level - 1),
      this.opts.quarantineMaxS,
    );
  }

  private openCircuit(prov: string, nowS: number, detail: string): void {
    const old = this.state.get(prov) || "closed";
    const level = (this.quarantineLevel.get(prov) || 0) + 1;
    this.quarantineLevel.set(prov, level);
    const backoff = this.backoff(level);
    this.state.set(prov, "open");
    this.openUntil.set(prov, nowS + backoff);
    this.heal({
      provider: prov,
      model: "",
      event: "circuit_opened",
      prev: old,
      next: "open",
      details: `${detail}; quarantine level ${level}, backoff ${backoff}s`,
    });
  }

  /**
   * Record a live request outcome. Mirrors Matrix.record()'s circuit half.
   * model is required so 404s can bench the model id precisely.
   */
  recordOutcome(prov: string, model: string, status: number, nowS = Date.now() / 1000): void {
    this.register(prov);
    if (status === 200) {
      const old = this.state.get(prov) || "closed";
      this.fails.set(prov, [0, nowS]);
      this.state.set(prov, "closed");
      this.quarantineLevel.set(prov, 0);
      if (old !== "closed") {
        this.heal({ provider: prov, model, event: "circuit_recovered", prev: old, next: "closed" });
      }
      return;
    }
    if (status === 429) {
      // Rate limits are counted (laneDead sees them) but NEVER open the
      // circuit by design — the lane is throttled, not dead.
      const [c] = this.fails.get(prov) || [0, 0];
      this.fails.set(prov, [c + 1, nowS]);
      return;
    }
    // Failure-class-aware counting (503-forensics 2026-09-21):
    // - 401/402 (dead key): the PROVIDER is dead — count 3x so the circuit
    //   opens and laneDead excludes it after a single attempt.
    // - 404: the MODEL is dead — bench it permanently (fail-fast, zero
    //   retry burn); the provider keeps serving healthy models, normal +1.
    // - 5xx/timeouts: transient until proven otherwise (+1).
    if (status === 404) this.noteEntitlement404(prov, model, nowS);
    const step = status === 401 || status === 402 ? 3 : 1;
    const [c] = this.fails.get(prov) || [0, 0];
    this.fails.set(prov, [c + step, nowS]);
    if (c + step >= CIRCUIT_OPEN_THRESHOLD) {
      this.openCircuit(prov, nowS, `${c + step} consecutive failures (step ${step})`);
    }
  }

  /**
   * recordProbe — synthetic liveness ping outcome. Updates strike counts
   * and the circuit WITHOUT touching Elo or request analytics (synthetic
   * rows would pollute latency percentiles and inflate Elo). Used by the
   * warm-standby pinger so a dead local backend quarantines before user
   * traffic hits it.
   */
  recordProbe(prov: string, ok: boolean, err = "", nowS = Date.now() / 1000): void {
    this.register(prov);
    if (ok) {
      this.fails.set(prov, [0, nowS]);
      if (this.state.get(prov) === "half") {
        this.state.set(prov, "closed");
        this.quarantineLevel.set(prov, 0);
        this.heal({ provider: prov, model: "warm-standby", event: "circuit_recovered", prev: "half", next: "closed" });
      }
      return;
    }
    const [c] = this.fails.get(prov) || [0, 0];
    this.fails.set(prov, [c + 1, nowS]);
    this.noteError(prov, 0, err || "probe failed", nowS);
    if (c + 1 >= CIRCUIT_OPEN_THRESHOLD && this.state.get(prov) !== "open") {
      this.openCircuit(prov, nowS, `${c + 1} consecutive probe failures`);
    }
  }

  /** Record a failure signature for /status (call on every failed attempt). */
  noteError(prov: string, status: number, err: string, nowS = Date.now() / 1000): void {
    this.lastError.set(prov, { status, err: String(err).slice(0, 300), at: nowS });
  }

  consecutiveFailures(prov: string): number {
    return this.fails.get(prov)?.[0] || 0;
  }

  circuitState(prov: string): CircuitState {
    return this.state.get(prov) || "closed";
  }

  /**
   * circuitOk — gate before sending traffic. An expired open circuit
   * lazily half-opens (admits one probe); half-open admits the probe.
   */
  circuitOk(prov: string, nowS = Date.now() / 1000): boolean {
    const st = this.state.get(prov) || "closed";
    if (st === "closed") return true;
    if (st === "open") {
      if (nowS > (this.openUntil.get(prov) || 0)) {
        this.state.set(prov, "half");
        return true;
      }
      return false;
    }
    return true; // half-open probe
  }

  /** Full quarantine/circuit snapshot for /status. */
  circuitInfo(prov: string, nowS = Date.now() / 1000): CircuitInfo {
    const until = this.openUntil.get(prov) || 0;
    const open = (this.state.get(prov) || "closed") === "open";
    return {
      state: this.state.get(prov) || "closed",
      quarantine_level: this.quarantineLevel.get(prov) || 0,
      consecutive_failures: this.consecutiveFailures(prov),
      backoff_s: open ? Math.max(0, Math.round(until - nowS)) : 0,
      open_until: open ? until : null,
      probe_in_s: open ? Math.max(0, Math.round(until - nowS)) : null,
      last_error: this.lastError.get(prov) || null,
    };
  }

  /**
   * laneDead — dead-lane exclusion for automatic race sets.
   * A provider whose last 6+ attempts ALL failed (no success resetting the
   * streak) within the last 10 minutes sits out of candidate pools. This
   * catches lanes the circuit intentionally ignores: persistent 429s,
   * shims whose /models still 200s while completions fail, hard 401/402s
   * between circuit openings. Self-healing: any success resets the counter
   * (recordOutcome), and the flag expires 10 minutes after the last
   * failure. Degraded mode (every lane dead) should still race the dead
   * lanes rather than serve 503 — the caller decides that, this is the signal.
   */
  laneDead(prov: string, nowS = Date.now() / 1000): boolean {
    const [c, ts] = this.fails.get(prov) || [0, 0];
    if (c < LANEDEAD_MIN_FAILS) return false;
    return nowS - ts < LANEDEAD_WINDOW_S;
  }

  /**
   * noteEntitlement404 — LOUD permanent bench for a 404'd model id.
   * A 404 on a model attempt means "no such model for this key" — delisted
   * or not entitled, and NEITHER heals by retrying. The model is benched
   * for the process lifetime after ONE 404: no retry burn.
   */
  noteEntitlement404(prov: string, model: string, nowS = Date.now() / 1000): void {
    const key = `${prov}:${model}`;
    if (this.entitlementDead.has(key)) return;
    this.entitlementDead.set(key, nowS);
    console.error(
      `[flock-policy] entitlement-404 BENCHED ${key} — no retry burn (re-admit: catalog relist)`,
    );
    this.heal({
      provider: prov,
      model,
      event: "entitlement_benched",
      prev: "live",
      next: "benched",
      details: "404 not-entitled/delisted: model benched for process lifetime, zero retries",
    });
  }

  /**
   * True when the model id was benched by a 404 (fail-fast, no attempts).
   * The bench clears automatically when the catalog re-admits the model
   * into its serving set — the catalog is the source of truth. Ids never
   * in the catalog keep the bench; there is no re-admission to observe.
   */
  isEntitlementDead(prov: string, model: string): boolean {
    const key = `${prov}:${model}`;
    if (!this.entitlementDead.has(key)) return false;
    try {
      if (this.opts.catalogServes(prov, model)) {
        this.entitlementDead.delete(key);
        return false;
      }
    } catch {
      // Catalog lookup failed; keep the bench (fail-closed).
    }
    return true;
  }

  // --- Flap tracker: models that repeatedly return empty (non-substantive)
  // completions collect strikes; FLAP_STRIKES within FLAP_WINDOW_S benches
  // the model out of candidate pools until the strikes decay. Re-entry is
  // automatic — the next race after decay re-probes the model for real.

  recordEmpty(prov: string, model: string, nowS = Date.now() / 1000): void {
    const k = `${prov}/${model}`;
    const cur = this.emptyStrikes.get(k);
    if (!cur || nowS - cur.last > FLAP_WINDOW_S) {
      this.emptyStrikes.set(k, { n: 1, last: nowS });
    } else {
      this.emptyStrikes.set(k, { n: cur.n + 1, last: nowS });
    }
  }

  emptyStrikeCount(prov: string, model: string, nowS = Date.now() / 1000): number {
    const k = `${prov}/${model}`;
    const cur = this.emptyStrikes.get(k);
    if (!cur) return 0;
    if (nowS - cur.last > FLAP_WINDOW_S) {
      this.emptyStrikes.delete(k);
      return 0;
    }
    return cur.n;
  }

  flapBanned(prov: string, model: string, nowS = Date.now() / 1000): boolean {
    return this.emptyStrikeCount(prov, model, nowS) >= FLAP_STRIKES;
  }

  /**
   * halfOpen — admit a provider to half-open for one probe's worth of
   * traffic (used by the quarantine prober after a successful re-probe).
   */
  halfOpen(prov: string): void {
    this.register(prov);
    if (this.state.get(prov) === "open") {
      this.state.set(prov, "half");
      this.heal({ provider: prov, model: "", event: "quarantine_probe_ok", prev: "open", next: "half", details: "active re-probe succeeded; admitting traffic" });
    }
  }

  /**
   * escalateQuarantine — re-open at the next backoff level after a failed
   * quarantine re-probe. Unlike recordProbe, this ALWAYS escalates (even
   * when already open): the backoff doubles per consecutive failed probe.
   */
  escalateQuarantine(prov: string, reason: string, nowS = Date.now() / 1000): void {
    this.register(prov);
    const old = this.state.get(prov) || "closed";
    const level = (this.quarantineLevel.get(prov) || 0) + 1;
    this.quarantineLevel.set(prov, level);
    const backoff = this.backoff(level);
    this.state.set(prov, "open");
    this.openUntil.set(prov, nowS + backoff);
    this.heal({
      provider: prov,
      model: "",
      event: "quarantine_probe_failed",
      prev: old,
      next: "open",
      details: `${reason}; quarantine level ${level}, backoff ${backoff}s`,
    });
  }

  providers(): string[] {
    return [...this.state.keys()];
  }
}

/**
 * startQuarantineProber — active health probing for quarantined providers.
 * Every intervalMs, each provider whose circuit is open and whose backoff
 * has expired gets ONE probe (per-attempt deadline lives inside `probe`).
 * Success half-opens the provider (admits traffic); failure re-opens at
 * the next backoff level. Returns a stop function.
 */
export function startQuarantineProber(
  policy: CircuitPolicy,
  probe: (provider: string) => Promise<boolean>,
  intervalMs = QUARANTINE_PROBE_MS,
): () => void {
  const tick = async () => {
    const nowS = Date.now() / 1000;
    for (const p of policy.providers()) {
      try {
        if (policy.circuitState(p) !== "open") continue;
        const info = policy.circuitInfo(p, nowS);
        if (info.probe_in_s !== null && info.probe_in_s > 0) continue;
        const ok = await probe(p);
        if (ok) {
          // Half-open: the next circuitOk() admits one probe's worth of
          // traffic; recordOutcome decides from there.
          policy.halfOpen(p);
        } else {
          policy.escalateQuarantine(p, "quarantine re-probe failed");
        }
      } catch (e) {
        console.error("[flock-policy] quarantine prober error for", p, e);
      }
    }
  };
  const timer = setInterval(() => {
    tick().catch((e) => console.error("[flock-policy] quarantine prober tick:", e));
  }, intervalMs);
  return () => clearInterval(timer);
}
