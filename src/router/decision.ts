import type { ChatBody, RouteResult } from "../strategy/router_types.ts";
import type { StrategyDeps } from "../strategy/types.ts";
import {
  tryLongctxPin,
  tryLongctx2MPin,
  longctxPinEnabled,
  longctx2MEnabled,
  estPromptTokens,
} from "../strategy/router_strategy.ts";
import { buildCoding, isLocalSwapModelId, LOCAL_ROLES } from "../strategy/catalog.ts";
import { freeCandidates } from "../strategy/strategy.ts";
import { pickStrategy, raceCandidates } from "./strategy.ts";

/**
 * decide()'s return value: the route result plus the routing metadata the
 * serving layer turns into response headers (X-Chalcopyrite-Strategy /
 * X-Routed-Via). RouteResult stays header-free on purpose — providers yield
 * status/body/timings only; the serving layer owns the HTTP envelope.
 */
export type Decision = RouteResult & { headers: Record<string, string> };

type Candidate = { readonly provider: string; readonly model: string };

/**
 * Local aliases (fast / quality / longctx / local-*) resolve to herd. Passing
 * `{}` here, as the previous build did, meant aliases were never recognized
 * as local and were mis-routed to the flock field.
 */
const LOCAL_CODING = buildCoding(LOCAL_ROLES, {});

/**
 * Long-context pin pipeline. 2M first (higher token gate), then 1M. First
 * hit wins. Both are direct keyed-lane calls outside the race; on
 * ineligibility or failure, `null` falls through to normal dispatch.
 *
 * NOTE: these still come from the deprecated router_strategy.ts (global
 * `state`). strategy/strategy.ts has no 2M pin yet; port it there, then
 * repoint this import and the deprecated chain can be deleted.
 */
async function tryPins(body: ChatBody, ctx: StrategyDeps): Promise<{ result: RouteResult; strategy: string } | null> {
  if (longctx2MEnabled()) {
    const estTokens = estPromptTokens(body);
    if (estTokens > 1_000_000) {
      const sessionId = ctx.stickyGet("")[1] ?? "";
      const result = await tryLongctx2MPin(body, sessionId);
      if (result != null) return { result, strategy: "longctx-2m-pin" };
    }
  }
  if (longctxPinEnabled()) {
    const sessionId = ctx.stickyGet("")[1] ?? "";
    const result = await tryLongctxPin(body, sessionId);
    if (result != null) return { result, strategy: "longctx-pin" };
  }
  return null;
}

/**
 * Candidate builder — pure. Bornite gets the local role matrix (fallback if
 * the live catalog hasn't recorded any local models yet). Rhodochrosite gets every
 * keyed, circuit-closed provider except bornite; if that's empty, free
 * candidates minus bornite.
 */
function buildCandidates(isLocal: boolean, ctx: StrategyDeps): Candidate[] {
  const out: Candidate[] = [];
  if (isLocal) {
    for (const model of ctx.servingModels("bornite")) out.push({ provider: "bornite", model });
    if (out.length === 0) {
      out.push({ provider: "bornite", model: LOCAL_ROLES.fast });
      out.push({ provider: "bornite", model: LOCAL_ROLES.quality });
      out.push({ provider: "bornite", model: LOCAL_ROLES.longctx });
    }
    return out;
  }
  for (const provider of ctx.providers()) {
    if (provider.name === "bornite") continue;
    if (!ctx.keyOk(provider.name)) continue;
    if (!ctx.circuitOk(provider.name)) continue;
    for (const model of ctx.servingModels(provider.name)) {
      out.push({ provider: provider.name, model });
    }
  }
  if (out.length === 0) {
    for (const [provider] of freeCandidates(ctx)) {
      if (provider === "bornite") continue;
      for (const model of ctx.servingModels(provider)) out.push({ provider, model });
    }
  }
  return out;
}

export async function decide(body: ChatBody, ctx: StrategyDeps): Promise<Decision> {
  const pinned = await tryPins(body, ctx);
  if (pinned) {
    return {
      ...pinned.result,
      headers: {
        "X-Chalcopyrite-Strategy": pinned.strategy,
        "X-Routed-Via": pinned.result.provider === "bornite" ? "bornite" : "rhodochrosite",
      },
    };
  }

  const model = typeof body.model === "string" ? body.model : undefined;
  const isLocal = model != null && isLocalSwapModelId(model, LOCAL_CODING);
  const strategyName = pickStrategy(body, ctx);
  const candidates = buildCandidates(isLocal, ctx);

  const result = await raceCandidates(
    candidates.map((c) => [c.provider, c.model] as [string, string]),
    { body, session: ctx.stickyGet("")[1] ?? "", strategyName },
  );

  return {
    ...result,
    headers: {
      "X-Chalcopyrite-Strategy": strategyName,
      "X-Routed-Via": isLocal ? "bornite" : "rhodochrosite",
    },
  };
}
