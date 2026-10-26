/**
 * flock/ts/policy/elo.ts — Elo rating policy ported from estate-scripts-router's
 * router_matrix.ts (Matrix.record / setElo / applyBenchPriors).
 *
 * Boundary with the Rust proxy: proxy/src/health.rs owns the persisted Elo
 * STORE (get_elo/set_elo on SQLite, default 1500). It has no rating math.
 * This module owns the math — outcome → rating updates, failure-class-aware
 * counting, the latency EMA, candidate scoring, and bench-prior seeding with
 * persisted-protection. Persistence flows through the injected EloStore, so
 * the same engine can write through to the Rust proxy's elo table, a JSON
 * file, or the PolicyHealthDB in health.ts. Best-effort everywhere: a store
 * failure must never break routing.
 *
 * Outcome rules (from Matrix.record):
 *   200        -> +16, latency EMA update (alpha 0.2), quarantine reset
 *   429        -> -8  (rate limits never open the circuit by design)
 *   401/402    -> dead key: counted 3x toward the circuit threshold (see
 *                circuit.ts), Elo -32 like any hard failure
 *   other fail -> -32, floor 100
 */

import { statSync, readFileSync } from "node:fs";

export const BASE_ELO = 1000;
export const MIN_ELO = 100;
export const K_WIN = 16;
export const K_FAIL = 32;
export const K_RATELIMIT = 8;
/** Latency EMA alpha: recent completions steer ordering, single outliers don't. */
export const LATENCY_EMA_ALPHA = 0.2;
/** candidateScore penalty: 300ms EMA -> -6pts; 17s EMA -> -340pts. */
export const LATENCY_PENALTY_DIVISOR = 50;

export interface BenchPriorsDoc {
  priors: Record<string, { elo?: number }>;
  generated_ts?: string;
}

export interface EloStore {
  /** Persist one provider's Elo. Must never throw. */
  save(provider: string, elo: number): void;
  /** Load all persisted Elos. Must never throw. */
  load(): Record<string, number>;
}

export class MemoryEloStore implements EloStore {
  private rows = new Map<string, number>();
  save(provider: string, elo: number): void {
    this.rows.set(provider, elo);
  }
  load(): Record<string, number> {
    return Object.fromEntries(this.rows);
  }
}

export interface ApplyPriorsResult {
  reloaded: boolean;
  reseeded: string[];
  restored: string[];
  source: string;
}

export class EloEngine {
  private elo = new Map<string, number>();
  private priorElo = new Map<string, number>();
  private latencyEmaMs = new Map<string, number>();
  /**
   * Providers with a durably persisted Elo row: restored at startup or
   * written by a live update through setElo(). Hot-reload NEVER re-seeds
   * these — even when the persisted value numerically equals the bench
   * prior. Numeric equality is not proof a provider is untouched.
   */
  private persisted = new Set<string>();
  private priorsMtime = 0;
  private priorsSource = "";
  private store: EloStore;

  constructor(store: EloStore = new MemoryEloStore()) {
    this.store = store;
  }

  get(provider: string): number {
    return this.elo.get(provider) ?? BASE_ELO;
  }

  latencyEma(provider: string): number | null {
    return this.latencyEmaMs.get(provider) ?? null;
  }

  /**
   * Latency-adjusted candidate score: Elo minus a latency penalty.
   * Elo moves +/-16..32 per request, so chronic slowness reliably demotes
   * a provider while the 0.2 EMA alpha keeps a single slow outlier from
   * deciding alone.
   */
  candidateScore(provider: string): number {
    return this.get(provider) - (this.latencyEmaMs.get(provider) || 0) / LATENCY_PENALTY_DIVISOR;
  }

  /** The ONLY writer of provider Elo. Writes through to the store. */
  private setElo(provider: string, value: number, markPersisted = true): void {
    this.elo.set(provider, value);
    if (markPersisted) this.persisted.add(provider);
    try {
      this.store.save(provider, value);
    } catch {
      /* persistence is best-effort; routing semantics are untouched */
    }
  }

  /**
   * Record one request outcome. Mirrors Matrix.record()'s Elo half:
   * +16 on success (with latency EMA), -8 on 429, -32 otherwise (floor 100).
   * Every live outcome marks the provider persisted, so bench-prior
   * hot-reloads never clobber learned values.
   */
  recordOutcome(provider: string, status: number, latencyMs: number): void {
    const cur = this.get(provider);
    if (status === 200) {
      this.setElo(provider, cur + K_WIN);
      const prev = this.latencyEmaMs.get(provider) ?? latencyMs;
      this.latencyEmaMs.set(
        provider,
        prev * (1 - LATENCY_EMA_ALPHA) + latencyMs * LATENCY_EMA_ALPHA,
      );
    } else if (status === 429) {
      this.setElo(provider, Math.max(MIN_ELO, cur - K_RATELIMIT));
    } else {
      this.setElo(provider, Math.max(MIN_ELO, cur - K_FAIL));
    }
  }

  /**
   * applyBenchPriors — seed Elo from bench-priors.json.
   *
   * Startup (force=true): providers with a persisted Elo row get their
   * live-learned value back and are protected; providers with no stored
   * row fall back to the bench prior (1000 when unbenched) IN MEMORY ONLY —
   * initial bench seeding is a fallback, not learned state, and is never
   * persisted. Only live outcome changes create stored rows.
   *
   * Hot-reload (force=false): re-seeds only providers that are neither
   * durably persisted nor touched by live traffic (cur === last prior).
   */
  applyBenchPriors(
    providers: string[],
    priors: Record<string, number>,
    mtime: number,
    source: string,
    force = false,
  ): ApplyPriorsResult {
    if (!force && mtime === this.priorsMtime) {
      return { reloaded: false, reseeded: [], restored: [], source: this.priorsSource };
    }
    let stored: Record<string, number> = {};
    if (force) {
      try {
        stored = this.store.load();
      } catch {
        /* corrupt/empty store — priors are the fallback */
      }
    }
    const reseeded: string[] = [];
    const restored: string[] = [];
    for (const p of providers) {
      const prior = priors[p] ?? BASE_ELO;
      if (force) {
        const prev = stored[p];
        if (typeof prev === "number") {
          // Restored live-learned Elo: map-only (already stored), marked
          // persisted so hot-reload never re-seeds it — even when the
          // stored value numerically equals the bench prior.
          this.elo.set(p, prev);
          this.priorElo.set(p, prior);
          this.persisted.add(p);
          restored.push(p);
        } else {
          // Missing row: bench prior fills the in-memory map ONLY. Never
          // persisted — fallback priors are not learned state.
          this.elo.set(p, prior);
          this.priorElo.set(p, prior);
          reseeded.push(p);
        }
      } else {
        // Durably persisted providers are never re-seeded, regardless of
        // numeric equality with the prior.
        if (this.persisted.has(p)) continue;
        const cur = this.elo.get(p) ?? BASE_ELO;
        const last = this.priorElo.get(p) ?? BASE_ELO;
        if (cur === last) {
          // Untouched by live traffic: refresh the baseline. Written to
          // the store but NOT marked persisted — a later priors refresh
          // may update it again; only learned values are protected.
          this.setElo(p, prior, false);
          this.priorElo.set(p, prior);
          reseeded.push(p);
        }
      }
    }
    this.priorsMtime = mtime;
    this.priorsSource = source;
    return { reloaded: true, reseeded, restored, source };
  }

  isPersisted(provider: string): boolean {
    return this.persisted.has(provider);
  }

  snapshot(): Record<string, number> {
    return Object.fromEntries(this.elo);
  }
}

/**
 * loadBenchPriors — read bench-priors.json the way router_matrix.ts does.
 * Missing or corrupt file fails open to {} (flat 1000 downstream).
 */
export function loadBenchPriors(path: string): {
  priors: Record<string, number>;
  source: string;
  mtime: number;
} {
  try {
    const mtime = statSync(path).mtimeMs;
    const doc = JSON.parse(readFileSync(path, "utf8")) as BenchPriorsDoc;
    const priors: Record<string, number> = {};
    for (const [p, pr] of Object.entries(doc.priors || {})) {
      const elo = (pr as { elo?: number }).elo;
      if (typeof elo === "number" && Number.isFinite(elo)) priors[p] = elo;
    }
    return { priors, source: doc.generated_ts || "unknown", mtime };
  } catch {
    return { priors: {}, source: "missing", mtime: 0 };
  }
}
