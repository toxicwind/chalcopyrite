#!/usr/bin/env bun
/**
 * catalog-sync — make the live catalog match reality.
 *
 * Probes both gateways (bornite, flock), then rewrites the live catalog:
 * every serving id no live gateway backs is moved to its provider's
 * quarantined list (disable-with-receipt — nothing is destroyed, ids come
 * back by re-running sync once their gateway serves them again).
 *
 *   chalcopyrite/bin/catalog-sync.ts            # sync + report
 *   chalcopyrite/bin/catalog-sync.ts --dry-run  # report only, no write
 */
import { readFileSync } from "node:fs";
import { audit, DEFAULT_REPORT_PATH } from "../src/audit";
import { writeLiveCatalog, DEFAULT_CATALOG_PATH } from "../src/catalog";

const args = process.argv.slice(2);
const flags = new Set(args.filter((a) => a.startsWith("--")));

const PORTS_ENV = process.env.ESTATE_PORTS_ENV ?? "/home/toxic/pens/estate/config/ports.env";
function portFromEnv(name: string, fallback: string): string {
  try {
    const text = readFileSync(PORTS_ENV, "utf8");
    const m = text.match(new RegExp(`^${name}=\\s*["']?([^"'\\s#]+)`, "m"));
    return m?.[1] ?? fallback;
  } catch {
    return fallback;
  }
}

function secretFrom(name: string): string {
  if (process.env[name]) return process.env[name]!;
  const home = process.env.HOME ?? "";
  try {
    const m = readFileSync(`${home}/.secrets`, "utf8").match(
      new RegExp(`^\\s*(?:export\\s+)?${name}\\s*=\\s*["']?([^"'\\n#]+)["']?`, "m"),
    );
    if (m?.[1]) return m[1].trim();
  } catch {
    /* no vault */
  }
  for (const file of [`${home}/.tau/flock.key`, `${home}/.tau/bornite.key`]) {
    try {
      const v = readFileSync(file, "utf8").trim();
      if (v) return v;
    } catch {
      /* absent */
    }
  }
  return "";
}

const result = await audit({
  bornite: `http://127.0.0.1:${portFromEnv("HERD_PORT", "25100")}`,
  borniteToken: secretFrom("HERD_KEY") || "bornite",
  flock: `http://127.0.0.1:${portFromEnv("FLOCK_ROUTER_PORT", "25193")}`,
  flockToken: secretFrom("FLOCK_KEY"),
});

if (!result.ok) {
  process.stderr.write(`catalog sync failed: ${result.reason}\n`);
  process.exit(1);
}

const { report } = result;
const live = new Set<string>();
for (const p of report.probes) {
  if (!p.ok) continue;
  for (const id of p.models) {
    live.add(id.toLowerCase());
    live.add(id.toLowerCase().split("/").at(-1) ?? "");
  }
}

let keptTotal = 0;
let quarantinedTotal = 0;
const providers: Record<string, { serving: string[]; quarantined: string[]; discovered: boolean }> = {};
for (const [name, entry] of Object.entries(report.liveCatalog.providers)) {
  const serving: string[] = [];
  const removed: string[] = [];
  for (const id of entry.serving) {
    const norm = id.toLowerCase();
    const bare = norm.split("/").at(-1) ?? "";
    if (live.has(norm) || live.has(bare)) serving.push(id);
    else removed.push(id);
  }
  providers[name] = {
    serving,
    quarantined: [...new Set([...entry.quarantined, ...removed])],
    discovered: entry.discovered,
  };
  keptTotal += serving.length;
  quarantinedTotal += removed.length;
  if (removed.length) {
    process.stdout.write(`${name}: kept ${serving.length}, quarantined ${removed.length} (e.g. ${removed.slice(0, 3).join(", ")})\n`);
  } else {
    process.stdout.write(`${name}: kept ${serving.length}, quarantined 0\n`);
  }
}

process.stdout.write(`\ntotal: kept ${keptTotal}, newly quarantined ${quarantinedTotal}\n`);

if (flags.has("--dry-run")) {
  process.stdout.write("dry-run: catalog not written\n");
} else {
  writeLiveCatalog(DEFAULT_CATALOG_PATH, {
    deadIds: report.liveCatalog.deadIds,
    providers,
  });
  process.stdout.write(`wrote ${DEFAULT_CATALOG_PATH} with the canonical contract\n`);
}
process.stdout.write(`report: ${DEFAULT_REPORT_PATH}\n`);
