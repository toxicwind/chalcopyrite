/**
 * flock/ts/policy/health.ts — provider health aggregation ported from
 * estate-scripts-router's router_health.ts (HealthDB, SQLite WAL).
 *
 * Boundary with the Rust proxy: proxy/src/health.rs owns live provider
 * health STATE (healthy/last-probe/latency EMA/stickies, write-behind
 * persisted to flock_state.db). This module owns request-level ANALYTICS:
 *
 *   - requests: every attempt (status, latency, strategy, winner, session)
 *   - model_health: 5-minute aggregate windows (success/fail/rate-limited,
 *     min/max/avg latency) — the 30-minute provider summaries and p50/p95
 *     percentiles read from these
 *   - healing_events: the circuit/quarantine/entitlement audit feed
 *     (circuit_opened, circuit_recovered, quarantine_probe_ok/failed,
 *     entitlement_benched, live)
 *   - rate_limit_events: 429 forensics
 *   - session_affinity: sticky routing with TTL
 *   - elo_state: durable Elo rows — implements the EloStore interface from
 *     elo.ts so EloEngine writes through here with zero extra wiring
 *
 * Probes must NOT write here (see CircuitPolicy.recordProbe / the
 * warm-standby sink): synthetic rows would pollute latency percentiles and
 * inflate Elo. Only live request outcomes call recordRequest.
 */

import { Database } from "bun:sqlite";
import { dirname } from "node:path";
import { statSync } from "node:fs";
import type { EloStore } from "./elo.ts";

export const STICKY_TTL_S = 1800;

export interface ProviderSummary {
  successes: number;
  failures: number;
  success_rate: number | null;
  avg_latency_ms: number | null;
  rate_limited: number;
}

export interface LatencyPercentiles {
  p50_ms: number | null;
  p95_ms: number | null;
  n: number;
}

export class PolicyHealthDB {
  conn: Database;
  private dbPath: string;

  constructor(path: string) {
    this.dbPath = path;
    const dir = dirname(path);
    try {
      Bun.spawnSync(["mkdir", "-p", dir]);
    } catch {
      /* ok */
    }
    this.conn = new Database(path);
    this.conn.exec("PRAGMA journal_mode=WAL");
    this.conn.exec("PRAGMA synchronous=NORMAL");
    this.migrate();
  }

  migrate(): void {
    this.conn.exec(`
      CREATE TABLE IF NOT EXISTS requests (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        ts REAL NOT NULL,
        provider TEXT NOT NULL,
        model TEXT NOT NULL,
        status INTEGER NOT NULL,
        latency_ms REAL NOT NULL,
        strategy TEXT NOT NULL DEFAULT '',
        winner INTEGER NOT NULL DEFAULT 0,
        session_id TEXT NOT NULL DEFAULT ''
      );
      CREATE INDEX IF NOT EXISTS idx_req_prov_model ON requests(provider, model);
      CREATE INDEX IF NOT EXISTS idx_req_ts ON requests(ts);

      CREATE TABLE IF NOT EXISTS model_health (
        provider TEXT NOT NULL,
        model TEXT NOT NULL,
        window_start REAL NOT NULL,
        successes INTEGER NOT NULL DEFAULT 0,
        failures INTEGER NOT NULL DEFAULT 0,
        rate_limited INTEGER NOT NULL DEFAULT 0,
        total_ms REAL NOT NULL DEFAULT 0,
        min_ms REAL NOT NULL DEFAULT 999999,
        max_ms REAL NOT NULL DEFAULT 0,
        PRIMARY KEY (provider, model, window_start)
      );

      CREATE TABLE IF NOT EXISTS healing_events (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        ts REAL NOT NULL,
        provider TEXT NOT NULL,
        model TEXT NOT NULL,
        event TEXT NOT NULL,
        prev_status TEXT NOT NULL DEFAULT '',
        new_status TEXT NOT NULL DEFAULT '',
        details TEXT NOT NULL DEFAULT ''
      );

      CREATE TABLE IF NOT EXISTS rate_limit_events (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        ts REAL NOT NULL,
        provider TEXT NOT NULL,
        model TEXT NOT NULL,
        status_code INTEGER NOT NULL,
        retry_after REAL DEFAULT NULL
      );

      CREATE TABLE IF NOT EXISTS session_affinity (
        session_id TEXT PRIMARY KEY,
        provider TEXT NOT NULL,
        model TEXT NOT NULL,
        updated_at REAL NOT NULL
      );

      CREATE TABLE IF NOT EXISTS elo_state (
        provider TEXT PRIMARY KEY,
        elo REAL NOT NULL,
        updated_at REAL NOT NULL
      );
    `);
    const cols = this.conn.query(`PRAGMA table_info(requests)`).all() as { name: string }[];
    if (!cols.some((c) => c.name === "est_tokens")) {
      this.conn.exec(`ALTER TABLE requests ADD COLUMN est_tokens REAL NOT NULL DEFAULT 0`);
    }
  }

  recordRequest(
    provider: string,
    model: string,
    status: number,
    latencyMs: number,
    strategy = "",
    winner = 0,
    sessionId = "",
    estTokens = 0,
  ): void {
    const now = Date.now() / 1000;
    const window = now - (now % 300);
    this.conn
      .query(
        `INSERT INTO requests (ts,provider,model,status,latency_ms,strategy,winner,session_id,est_tokens)
         VALUES (?,?,?,?,?,?,?,?,?)`,
      )
      .run(now, provider, model, status, latencyMs, strategy, winner, sessionId, estTokens);

    if (status === 200) {
      this.conn
        .query(
          `INSERT INTO model_health
           (provider,model,window_start,successes,failures,total_ms,min_ms,max_ms)
           VALUES (?,?,?,1,0,?,?,?)
           ON CONFLICT(provider,model,window_start) DO UPDATE SET
             successes=successes+1, total_ms=total_ms+excluded.total_ms,
             min_ms=min(min_ms,excluded.min_ms), max_ms=max(max_ms,excluded.max_ms)`,
        )
        .run(provider, model, window, latencyMs, latencyMs, latencyMs);
    } else if (status === 429) {
      this.conn
        .query(
          `INSERT INTO model_health
           (provider,model,window_start,successes,failures,rate_limited,total_ms,min_ms,max_ms)
           VALUES (?,?,?,0,0,1,?,?,?)
           ON CONFLICT(provider,model,window_start) DO UPDATE SET
             rate_limited=rate_limited+1`,
        )
        .run(provider, model, window, latencyMs, latencyMs, latencyMs);
    } else {
      this.conn
        .query(
          `INSERT INTO model_health
           (provider,model,window_start,successes,failures,total_ms,min_ms,max_ms)
           VALUES (?,?,?,0,1,?,?,?)
           ON CONFLICT(provider,model,window_start) DO UPDATE SET
             failures=failures+1, total_ms=total_ms+excluded.total_ms,
             min_ms=min(min_ms,excluded.min_ms), max_ms=max(max_ms,excluded.max_ms)`,
        )
        .run(provider, model, window, latencyMs, latencyMs, latencyMs);
    }
  }

  recordRateLimit(provider: string, model: string, statusCode: number): void {
    this.conn
      .query(
        `INSERT INTO rate_limit_events (ts,provider,model,status_code,retry_after)
         VALUES (?,?,?,?,NULL)`,
      )
      .run(Date.now() / 1000, provider, model, statusCode);
  }

  recordHealing(
    provider: string,
    model: string,
    event: string,
    prevStatus = "",
    newStatus = "",
    details = "",
  ): void {
    this.conn
      .query(
        `INSERT INTO healing_events
         (ts,provider,model,event,prev_status,new_status,details)
         VALUES (?,?,?,?,?,?,?)`,
      )
      .run(Date.now() / 1000, provider, model, event, prevStatus, newStatus, details);
  }

  saveElo(provider: string, elo: number): void {
    this.conn
      .query(
        `INSERT INTO elo_state (provider, elo, updated_at)
         VALUES (?,?,?)
         ON CONFLICT(provider) DO UPDATE SET
           elo=excluded.elo, updated_at=excluded.updated_at`,
      )
      .run(provider, elo, Date.now() / 1000);
  }

  loadElo(): Record<string, number> {
    const rows = this.conn.query(`SELECT provider, elo FROM elo_state`).all() as {
      provider: string;
      elo: number;
    }[];
    const out: Record<string, number> = {};
    for (const r of rows) {
      if (typeof r.elo === "number" && Number.isFinite(r.elo)) out[r.provider] = r.elo;
    }
    return out;
  }

  /** EloStore adapter: lets EloEngine write through to this DB. */
  asEloStore(): EloStore {
    return {
      save: (provider, elo) => {
        try {
          this.saveElo(provider, elo);
        } catch {
          /* best-effort */
        }
      },
      load: () => {
        try {
          return this.loadElo();
        } catch {
          return {};
        }
      },
    };
  }

  /** Per-provider success/failure/latency summary over the last 30 min. */
  getProviderSummary(): Record<string, ProviderSummary> {
    const cutoff = Date.now() / 1000 - 1800;
    const rows = this.conn
      .query(
        `SELECT provider AS provider,
                SUM(successes) AS successes,
                SUM(failures) AS failures,
                AVG(total_ms / max(successes+failures,1)) AS avg_ms,
                SUM(rate_limited) AS rate_limited
         FROM model_health
         WHERE window_start>=? GROUP BY provider`,
      )
      .all(cutoff) as {
      provider: string;
      successes: number;
      failures: number;
      avg_ms: number | null;
      rate_limited: number;
    }[];
    const result: Record<string, ProviderSummary> = {};
    for (const r of rows) {
      const s = r.successes || 0;
      const f = r.failures || 0;
      const total = s + f;
      result[r.provider] = {
        successes: s,
        failures: f,
        success_rate: total > 0 ? Math.round((s / total) * 1000) / 1000 : null,
        avg_latency_ms: r.avg_ms != null ? Math.round(Number(r.avg_ms) * 10) / 10 : null,
        rate_limited: r.rate_limited || 0,
      };
    }
    return result;
  }

  /** p50/p95 of successful request latency per provider, last 30 min. */
  getLatencyPercentiles(): Record<string, LatencyPercentiles> {
    const cutoff = Date.now() / 1000 - 1800;
    const out: Record<string, LatencyPercentiles> = {};
    const provs = this.conn
      .query(`SELECT DISTINCT provider FROM requests WHERE ts>=?`)
      .all(cutoff) as { provider: string }[];
    for (const { provider } of provs) {
      const rows = this.conn
        .query(
          `SELECT latency_ms FROM requests
           WHERE provider=? AND ts>=? AND status=200
           ORDER BY latency_ms`,
        )
        .all(provider, cutoff) as { latency_ms: number }[];
      if (!rows.length) {
        out[provider] = { p50_ms: null, p95_ms: null, n: 0 };
        continue;
      }
      // `rows` is non-empty (guarded above) and the index is clamped into
      // [0, rows.length - 1], so the lookup is always in range.
      const q = (p: number) => rows[Math.min(rows.length - 1, Math.floor(p * rows.length))]!.latency_ms;
      const r2 = (v: number) => Math.round(v * 10) / 10;
      out[provider] = { p50_ms: r2(q(0.5)), p95_ms: r2(q(0.95)), n: rows.length };
    }
    return out;
  }

  stickyGet(sessionId: string, ttl = STICKY_TTL_S): [string | null, string | null] {
    const cutoff = Date.now() / 1000 - ttl;
    const row = this.conn
      .query(
        `SELECT provider, model FROM session_affinity
         WHERE session_id=? AND updated_at>=?`,
      )
      .get(sessionId, cutoff) as { provider: string; model: string } | null;
    return row ? [row.provider, row.model] : [null, null];
  }

  stickySet(sessionId: string, provider: string, model: string): void {
    this.conn
      .query(
        `INSERT INTO session_affinity (session_id, provider, model, updated_at)
         VALUES (?,?,?,?)
         ON CONFLICT(session_id) DO UPDATE SET
           provider=excluded.provider, model=excluded.model, updated_at=excluded.updated_at`,
      )
      .run(sessionId, provider, model, Date.now() / 1000);
  }

  recentHealing(provider: string, limit = 10) {
    return this.conn
      .query(
        `SELECT ts,model,event,prev_status,new_status,details
         FROM healing_events WHERE provider=?
         ORDER BY ts DESC LIMIT ?`,
      )
      .all(provider, limit);
  }

  mtime(): number {
    return statSync(this.dbPath).mtimeMs;
  }
}
