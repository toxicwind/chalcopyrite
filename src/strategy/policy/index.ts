/**
 * flock/ts/policy — estate-scripts-router policy engine, ported to TypeScript.
 *
 * The Rust proxy (mesh/proxy/flock-proxy/src) owns the hot path: per-request circuit
 * breaking (circuit.rs), persisted health/Elo store (health.rs), routing
 * decisions (decision.rs), model-pressure governing (governor.rs). This
 * package is the POLICY tier above it — the rating math, quarantine
 * strategy, warm standby, and analytics the proxy deliberately does not do:
 *
 *   elo.ts          — Elo rating math: outcome → rating updates, latency EMA,
 *                     candidate scoring, bench-prior seeding with
 *                     persisted-protection. (Rust only stores the value.)
 *   bench-priors.json — bench-derived Elo seeds (GuideLLM v3 quality,
 *                     MODEL-MAX liveness/latency, router-proof availability).
 *   circuit.ts      — quarantine policy: failure-class counting, exponential
 *                     backoff levels, active re-probing, probe-only outcomes,
 *                     laneDead exclusion, entitlement-404 bench, flap tracker.
 *   warm-standby.ts — local-lane warm standby pinger with keyed-provider
 *                     auth (nim-proxy client key on probes).
 *   health.ts       — request-level analytics: 5-min aggregate windows,
 *                     30-min summaries, p50/p95 percentiles, healing feed,
 *                     sticky affinity, durable elo_state (EloStore adapter).
 */

export * from "./elo.ts";
export * from "./circuit.ts";
export * from "./warm-standby.ts";
export * from "./health.ts";
