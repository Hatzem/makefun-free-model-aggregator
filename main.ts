// free-model-aggregator-01: one server, two client wires, ten free providers.
//
// The server reads the trial brief (provider directory, sealed quality table,
// workload manifest), probes every provider's catalog route, registers one
// catalog with the env, and serves the env client's 20 requests one at a time.
// Each request is answered by the best free model that meets it, in the wire
// the client used. When a provider refuses (402/429/5xx) the request is
// retried on the next best free model that meets it.
//
// Design notes:
//   - Routing is rating-first: candidates are the free models meeting the
//     request's requirements, ordered by the sealed rating_milli. A provider
//     that fails twice in a row is benched for the rest of the trial (a 402
//     benches it at once: that provider's budget is spent). Budgets are
//     never published, so the bench rule is what keeps the server off dead
//     providers without ever needing to know a budget.
//   - A JSON-mode request is never sent to an anthropic-wire provider: that
//     wire cannot carry response_format, so the provider would echo json_mode
//     absent and the request would fail the echo check.
//   - Every upstream answer is buffered and re-rendered in the client's
//     wire. That keeps every response well-formed (usage always present,
//     streaming event order always right) whatever wire answered it.
//   - Nothing here reads the wall clock or unseeded randomness: every choice
//     is a function of the brief, the requests and the providers' answers,
//     so a replayed trial leaves the same state export.

import { createServer, type IncomingMessage, type Server, type ServerResponse } from 'node:http';
import { readFileSync } from 'node:fs';

type Json = Record<string, unknown>;
const isRecord = (v: unknown): v is Json => v !== null && typeof v === 'object' && !Array.isArray(v);
const asString = (v: unknown): string => (typeof v === 'string' ? v : '');
const asNumber = (v: unknown): number | null => (typeof v === 'number' && Number.isFinite(v) ? v : null);
const asBool = (v: unknown): boolean | null => (typeof v === 'boolean' ? v : null);

type Wire = 'openai' | 'anthropic' | 'gemini';
type AuthScheme = 'bearer' | 'x-api-key' | 'x-goog-api-key' | 'none';

const PROTOCOL_VERSION = 1;
const POLL_MS = 100;
const PROBE_TIMEOUT_MS = 5000;
const UPSTREAM_TIMEOUT_MS = 8000;
const MAX_BODY_BYTES = 32 * 1024 * 1024;
const FIXED_CREATED = 1767225600;
const FIXED_CREATED_AT = '2026-01-01T00:00:00Z';
const ANTHROPIC_VERSION = '2023-06-01';

const ENV_URL = (process.env.MAKEFUN_ENV_URL ?? '').replace(/\/+$/, '');
const PROMPT_FILE = process.env.MAKEFUN_PROMPT_FILE ?? '';

function log(message: string): void {
  process.stderr.write(`aggregator: ${message}\n`);
}

// ---------------------------------------------------------------------------
// The brief: three JSON documents found by their `schema` member.
// ---------------------------------------------------------------------------

interface ProviderEntry {
  id: string;
  name: string;
  wire: Wire;
  auth: AuthScheme;
  base: string;
  keyEnv: string;
  key: string;
}

interface RatingRow {
  model: string;
  provider: string;
  rating: number;
  context: number | null;
  tools: boolean;
  vision: boolean;
  json: boolean;
  free: boolean;
}

interface Requirement {
  tools?: boolean;
  vision?: boolean;
  json?: boolean;
  minContext?: number;
}

interface ManifestEntry {
  id: string;
  api: 'openai' | 'anthropic';
  stream: boolean;
  weight: number;
  requires: Requirement;
}

interface Brief {
  providers: ProviderEntry[];
  ratings: RatingRow[];
  best: Map<string, number>;
  manifest: ManifestEntry[];
  error: string | null;
}

function objectEnd(text: string, start: number): number {
  let depth = 0;
  let inString = false;
  for (let i = start; i < text.length; i++) {
    const ch = text[i];
    if (inString) {
      if (ch === '\\') i += 1;
      else if (ch === '"') inString = false;
    } else if (ch === '"') inString = true;
    else if (ch === '{') depth += 1;
    else if (ch === '}') {
      depth -= 1;
      if (depth === 0) return i;
    }
  }
  return -1;
}

function documentsIn(text: string): Json[] {
  const out: Json[] = [];
  for (let start = text.indexOf('{'); start !== -1; start = text.indexOf('{', start + 1)) {
    const end = objectEnd(text, start);
    if (end === -1) continue;
    try {
      const value: unknown = JSON.parse(text.slice(start, end + 1));
      if (isRecord(value) && typeof value.schema === 'string') out.push(value);
    } catch {
      // Not JSON on its own: look on.
    }
  }
  return out;
}

function readBrief(path: string): Brief {
  const empty: Brief = { providers: [], ratings: [], best: new Map(), manifest: [], error: null };
  if (path === '') return { ...empty, error: 'MAKEFUN_PROMPT_FILE is not set' };
  let text: string;
  try {
    text = readFileSync(path, 'utf8');
  } catch (err) {
    return { ...empty, error: `cannot read ${path}: ${(err as Error).message}` };
  }
  const docs = documentsIn(text);
  const dirDoc = docs.find((d) => d.schema === 'free-model-aggregator-01/providers/v1' && Array.isArray(d.providers));
  if (!dirDoc || !Array.isArray(dirDoc.providers)) {
    return { ...empty, error: 'the brief holds no provider directory document' };
  }
  const providers: ProviderEntry[] = [];
  for (const raw of dirDoc.providers) {
    if (!isRecord(raw)) continue;
    const id = asString(raw.id);
    const wire = raw.wire === 'openai' || raw.wire === 'anthropic' || raw.wire === 'gemini' ? raw.wire : null;
    if (id === '' || wire === null) continue;
    const auth: AuthScheme =
      raw.auth === 'bearer' || raw.auth === 'x-api-key' || raw.auth === 'x-goog-api-key' || raw.auth === 'none' ? raw.auth : 'bearer';
    const base = asString(raw.path) !== '' ? asString(raw.path) : asString(raw.base_path) !== '' ? asString(raw.base_path) : asString(raw.base_url) !== '' ? asString(raw.base_url) : asString(raw.base);
    providers.push({ id, name: asString(raw.name) || id, wire, auth, base, keyEnv: asString(raw.key_env), key: asString(raw.key) });
  }
  const ratings: RatingRow[] = [];
  const best = new Map<string, number>();
  const ratingsDoc = docs.find((d) => d.schema === 'free-model-aggregator-01/ratings/v1' && Array.isArray(d.ratings));
  if (ratingsDoc && Array.isArray(ratingsDoc.ratings)) {
    for (const raw of ratingsDoc.ratings) {
      if (!isRecord(raw)) continue;
      const model = asString(raw.model);
      const provider = asString(raw.provider);
      if (model === '' || provider === '') continue;
      ratings.push({
        model,
        provider,
        rating: asNumber(raw.rating_milli) ?? 0,
        context: asNumber(raw.context_length),
        tools: raw.tools === true,
        vision: raw.vision === true,
        json: raw.json === true,
        free: raw.free === true,
      });
    }
  }
  if (ratingsDoc && Array.isArray(ratingsDoc.best)) {
    for (const raw of ratingsDoc.best) {
      if (isRecord(raw) && asString(raw.id) !== '') best.set(asString(raw.id), asNumber(raw.rating_milli) ?? 0);
    }
  }
  const manifest: ManifestEntry[] = [];
  const workloadDoc = docs.find((d) => d.schema === 'free-model-aggregator-01/workload/v1' && Array.isArray(d.manifest));
  if (workloadDoc && Array.isArray(workloadDoc.manifest)) {
    for (const raw of workloadDoc.manifest) {
      if (!isRecord(raw) || asString(raw.id) === '') continue;
      const req = isRecord(raw.requires) ? raw.requires : {};
      manifest.push({
        id: asString(raw.id),
        api: raw.api === 'anthropic' ? 'anthropic' : 'openai',
        stream: raw.stream === true,
        weight: raw.weight === 2 ? 2 : 1,
        requires: {
          tools: req.tools === true,
          vision: req.vision === true,
          json: req.json === true,
          ...(typeof req.min_context === 'number' && Number.isFinite(req.min_context) ? { minContext: req.min_context } : {}),
        },
      });
    }
  }
  return { providers, ratings, best, manifest, error: null };
}

// ---------------------------------------------------------------------------
// Catalog probe: each provider's own catalog route, in its own wire.
// ---------------------------------------------------------------------------

interface ListedModel {
  id: string;
  displayName: string;
  free: boolean | null;
  context: number | null;
  tools: boolean | null;
  vision: boolean | null;
  json: boolean | null;
}

interface Probe {
  provider: ProviderEntry;
  ok: boolean;
  models: ListedModel[];
  error: string | null;
}

function joinBase(envUrl: string, base: string): string {
  const b = base.trim();
  if (/^https?:\/\//i.test(b)) return b.replace(/\/+$/, '');
  const prefix = envUrl.replace(/\/+$/, '');
  if (b === '') return prefix;
  return `${prefix}${b.startsWith('/') ? '' : '/'}${b.replace(/\/+$/, '')}`;
}

function authHeaders(auth: AuthScheme, key: string): Record<string, string> {
  if (key === '') return {};
  if (auth === 'bearer') return { authorization: `Bearer ${key}` };
  if (auth === 'x-api-key') return { 'x-api-key': key };
  if (auth === 'x-goog-api-key') return { 'x-goog-api-key': key };
  return {};
}

function listedModel(wire: Wire, v: unknown): ListedModel | null {
  if (!isRecord(v)) return null;
  const id = wire === 'gemini' ? asString(v.name).replace(/^models\//, '') : asString(v.id);
  if (id === '') return null;
  const context = asNumber(v.context_length) ?? asNumber(v.context_window) ?? asNumber(v.inputTokenLimit);
  let free: boolean | null = asBool(v.free);
  if (free === null && isRecord(v.pricing)) {
    const values = Object.values(v.pricing).flatMap((x) => (typeof x === 'number' ? [x] : typeof x === 'string' && x.trim() !== '' ? [Number(x)] : []));
    if (values.length > 0) free = values.every((n) => Number.isFinite(n) && n === 0);
  }
  return {
    id,
    displayName: asString(v.display_name) !== '' ? asString(v.display_name) : asString(v.displayName) !== '' ? asString(v.displayName) : id,
    free,
    context,
    tools: asBool(v.tools),
    vision: asBool(v.vision),
    json: asBool(v.json),
  };
}

async function probeProvider(provider: ProviderEntry): Promise<Probe> {
  const base = joinBase(ENV_URL, provider.base);
  const url = provider.wire === 'gemini' ? `${base}/v1beta/models` : `${base}/v1/models`;
  const headers: Record<string, string> = { accept: 'application/json', ...authHeaders(provider.auth, provider.key) };
  if (provider.wire === 'anthropic') headers['anthropic-version'] = ANTHROPIC_VERSION;
  try {
    const res = await fetch(url, { headers, signal: AbortSignal.timeout(PROBE_TIMEOUT_MS) });
    if (!res.ok) return { provider, ok: false, models: [], error: `GET ${url} answered HTTP ${res.status}` };
    const body: unknown = await res.json();
    if (!isRecord(body)) return { provider, ok: false, models: [], error: `GET ${url} answered a non-object` };
    const list = provider.wire === 'gemini' ? body.models : body.data;
    if (!Array.isArray(list)) return { provider, ok: false, models: [], error: `GET ${url} listed no models` };
    const models = list.flatMap((v) => {
      const m = listedModel(provider.wire, v);
      return m === null ? [] : [m];
    });
    return { provider, ok: true, models, error: null };
  } catch (err) {
    return { provider, ok: false, models: [], error: `GET ${url} failed: ${(err as Error).message}` };
  }
}

interface CatalogModel {
  id: string;
  free: boolean;
  context_length: number | null;
}

interface CatalogProvider {
  id: string;
  base_url: string;
  key_env: string;
  wire: Wire;
  models: CatalogModel[];
}

interface Catalog {
  providers: CatalogProvider[];
}

function ratingRow(brief: Brief, providerId: string, modelId: string): RatingRow | null {
  return brief.ratings.find((r) => r.provider === providerId && r.model === modelId) ?? null;
}

function modelIsFree(model: ListedModel, row: RatingRow | null): boolean {
  if (row !== null) return row.free;
  return model.free ?? false;
}

interface Caps {
  context: number | null;
  tools: boolean | null;
  vision: boolean | null;
  json: boolean | null;
}

function capsOf(model: ListedModel, row: RatingRow | null): Caps {
  return {
    context: row?.context ?? model.context ?? null,
    tools: row?.tools ?? model.tools ?? null,
    vision: row?.vision ?? model.vision ?? null,
    json: row?.json ?? model.json ?? null,
  };
}

function meetsRequirements(req: Requirement, caps: Caps): boolean {
  if (req.tools === true && caps.tools !== true) return false;
  if (req.vision === true && caps.vision !== true) return false;
  if (req.json === true && caps.json !== true) return false;
  if (req.minContext !== undefined && (caps.context === null || caps.context < req.minContext)) return false;
  return true;
}

function buildCatalog(brief: Brief, probes: readonly Probe[]): Catalog {
  const providers: CatalogProvider[] = [];
  for (const probe of probes) {
    if (!probe.ok) continue;
    const seen = new Set<string>();
    const models: CatalogModel[] = [];
    for (const model of [...probe.models].sort((a, b) => (a.id < b.id ? -1 : a.id > b.id ? 1 : 0))) {
      if (seen.has(model.id)) continue;
      seen.add(model.id);
      const row = ratingRow(brief, probe.provider.id, model.id);
      models.push({ id: model.id, free: modelIsFree(model, row), context_length: row?.context ?? model.context ?? null });
    }
    providers.push({ id: probe.provider.id, base_url: probe.provider.base, key_env: probe.provider.keyEnv, wire: probe.provider.wire, models });
  }
  return { providers };
}

// ---------------------------------------------------------------------------
// Routing: rating-first candidates, bench on repeated failure.
// ---------------------------------------------------------------------------

interface Candidate {
  probe: Probe;
  model: ListedModel;
  row: RatingRow | null;
  rating: number;
}

interface ProviderState {
  calls: number;
  consecFail: number;
  benched: boolean;
}

const providerState = new Map<string, ProviderState>();

function stateOf(providerId: string): ProviderState {
  let s = providerState.get(providerId);
  if (s === undefined) {
    s = { calls: 0, consecFail: 0, benched: false };
    providerState.set(providerId, s);
  }
  return s;
}

function candidatesFor(brief: Brief, probes: readonly Probe[], req: Requirement): Candidate[] {
  const out: Candidate[] = [];
  for (const probe of probes) {
    if (!probe.ok) continue;
    for (const model of probe.models) {
      const row = ratingRow(brief, probe.provider.id, model.id);
      if (!modelIsFree(model, row)) continue;
      if (!meetsRequirements(req, capsOf(model, row))) continue;
      // The anthropic wire cannot carry response_format, so a JSON-mode
      // request would echo json_mode absent there: never route one to it.
      if (req.json === true && probe.provider.wire === 'anthropic') continue;
      out.push({ probe, model, row, rating: row?.rating ?? 0 });
    }
  }
  out.sort((a, b) => b.rating - a.rating || (a.probe.provider.id < b.probe.provider.id ? -1 : a.probe.provider.id > b.probe.provider.id ? 1 : a.model.id < b.model.id ? -1 : a.model.id > b.model.id ? 1 : 0));
  return out;
}

// ---------------------------------------------------------------------------
// Wire translation: request bodies.
// ---------------------------------------------------------------------------

function textOf(content: unknown): string {
  if (typeof content === 'string') return content;
  if (!Array.isArray(content)) return '';
  return content.flatMap((b) => (isRecord(b) && b.type === 'text' && typeof b.text === 'string' ? [b.text] : [])).join('\n');
}

function argsObject(value: unknown): Json {
  if (isRecord(value)) return value;
  if (typeof value === 'string' && value.trim() !== '') {
    try {
      const parsed: unknown = JSON.parse(value);
      if (isRecord(parsed)) return parsed;
    } catch {
      // Not JSON: an empty input.
    }
  }
  return {};
}

interface ImageParts {
  mime: string;
  data: string;
  url: string;
}

function imagePartsOf(url: string): ImageParts | null {
  const data = /^data:([^;,]*);base64,(.*)$/s.exec(url);
  if (data !== null) return { mime: data[1] !== '' ? data[1] : 'image/png', data: data[2], url: '' };
  if (url !== '') return { mime: '', data: '', url };
  return null;
}

/** OpenAI messages as an Anthropic Messages body. */
function openAIToAnthropic(body: Json, model: string): Json {
  const messages: Json[] = [];
  const systemParts: string[] = [];
  for (const m of Array.isArray(body.messages) ? body.messages : []) {
    if (isRecord(m) && m.role === 'system') {
      const text = textOf(m.content);
      if (text !== '') systemParts.push(text);
    }
  }
  const system = systemParts.join('\n');
  const source = Array.isArray(body.messages) ? body.messages : [];
  for (const raw of source) {
    if (!isRecord(raw)) continue;
    if (raw.role === 'system') continue;
    if (raw.role === 'assistant') {
      const blocks: Json[] = [];
      const text = textOf(raw.content);
      if (text !== '') blocks.push({ type: 'text', text });
      const calls = Array.isArray(raw.tool_calls) ? raw.tool_calls : [];
      for (const call of calls) {
        if (!isRecord(call) || !isRecord(call.function)) continue;
        blocks.push({
          type: 'tool_use',
          id: asString(call.id) !== '' ? asString(call.id) : 'toolu_unknown',
          name: asString(call.function.name),
          input: argsObject(call.function.arguments),
        });
      }
      if (blocks.length > 0) messages.push({ role: 'assistant', content: blocks });
      continue;
    }
    if (raw.role === 'tool') {
      messages.push({
        role: 'user',
        content: [{ type: 'tool_result', tool_use_id: asString(raw.tool_call_id), content: typeof raw.content === 'string' ? raw.content : textOf(raw.content) }],
      });
      continue;
    }
    if (raw.role !== 'user') continue;
    const content = raw.content;
    if (typeof content === 'string') {
      messages.push({ role: 'user', content });
      continue;
    }
    if (!Array.isArray(content)) continue;
    const blocks: Json[] = [];
    for (const part of content) {
      if (!isRecord(part)) continue;
      if (part.type === 'text' && typeof part.text === 'string') blocks.push({ type: 'text', text: part.text });
      else if (part.type === 'image_url' && isRecord(part.image_url) && typeof part.image_url.url === 'string') {
        const img = imagePartsOf(part.image_url.url);
        if (img !== null) {
          blocks.push(
            img.data !== ''
              ? { type: 'image', source: { type: 'base64', media_type: img.mime, data: img.data } }
              : { type: 'image', source: { type: 'url', url: img.url } },
          );
        }
      }
    }
    if (blocks.length > 0) messages.push({ role: 'user', content: blocks });
  }
  const out: Json = { model, messages };
  if (system !== '') out.system = system;
  if (typeof body.max_tokens === 'number') out.max_tokens = body.max_tokens;
  else if (typeof body.max_completion_tokens === 'number') out.max_tokens = body.max_completion_tokens;
  if (typeof body.temperature === 'number') out.temperature = body.temperature;
  if (typeof body.top_p === 'number') out.top_p = body.top_p;
  if (Array.isArray(body.stop) && body.stop.length > 0) out.stop_sequences = body.stop.filter((s) => typeof s === 'string');
  if (Array.isArray(body.tools)) {
    const tools = body.tools.flatMap((raw) => {
      if (!isRecord(raw) || !isRecord(raw.function) || typeof raw.function.name !== 'string') return [];
      return [{ name: raw.function.name, description: asString(raw.function.description), input_schema: isRecord(raw.function.parameters) ? raw.function.parameters : { type: 'object', properties: {} } }];
    });
    if (tools.length > 0) out.tools = tools;
  }
  const choice = body.tool_choice;
  if (choice === 'auto') out.tool_choice = { type: 'auto' };
  else if (choice === 'required') out.tool_choice = { type: 'any' };
  else if (isRecord(choice) && choice.type === 'function' && isRecord(choice.function) && typeof choice.function.name === 'string') out.tool_choice = { type: 'tool', name: choice.function.name };
  if (body.stream === true) out.stream = true;
  return out;
}

/** Anthropic messages as an OpenAI Chat Completions body. */
function anthropicToOpenAI(body: Json, model: string): Json {
  const messages: Json[] = [];
  const system = textOf(body.system);
  if (system !== '') messages.push({ role: 'system', content: system });
  const source = Array.isArray(body.messages) ? body.messages : [];
  for (const raw of source) {
    if (!isRecord(raw)) continue;
    const assistant = raw.role === 'assistant';
    const content = raw.content;
    if (typeof content === 'string') {
      messages.push({ role: assistant ? 'assistant' : 'user', content });
      continue;
    }
    if (!Array.isArray(content)) continue;
    if (assistant) {
      const toolCalls: Json[] = [];
      for (const block of content) {
        if (!isRecord(block) || block.type !== 'tool_use') continue;
        toolCalls.push({
          id: asString(block.id) !== '' ? asString(block.id) : '',
          type: 'function',
          function: { name: asString(block.name), arguments: JSON.stringify(isRecord(block.input) ? block.input : {}) },
        });
      }
      const text = textOf(content);
      messages.push({ role: 'assistant', content: text !== '' ? text : null, ...(toolCalls.length > 0 ? { tool_calls: toolCalls } : {}) });
      continue;
    }
    const parts: Json[] = [];
    for (const block of content) {
      if (!isRecord(block)) continue;
      if (block.type === 'tool_result') {
        messages.push({ role: 'tool', tool_call_id: asString(block.tool_use_id), content: typeof block.content === 'string' ? block.content : textOf(block.content) });
        continue;
      }
      if (block.type === 'text' && typeof block.text === 'string') parts.push({ type: 'text', text: block.text });
      else if (block.type === 'image' && isRecord(block.source)) {
        const src = block.source;
        if (src.type === 'base64' && typeof src.data === 'string') {
          const media = typeof src.media_type === 'string' ? src.media_type : 'image/png';
          parts.push({ type: 'image_url', image_url: { url: `data:${media};base64,${src.data}` } });
        } else if (src.type === 'url' && typeof src.url === 'string') parts.push({ type: 'image_url', image_url: { url: src.url } });
      }
    }
    if (parts.length > 0) messages.push({ role: 'user', content: parts });
  }
  const out: Json = { model, messages };
  if (typeof body.max_tokens === 'number') out.max_tokens = body.max_tokens;
  if (typeof body.temperature === 'number') out.temperature = body.temperature;
  if (typeof body.top_p === 'number') out.top_p = body.top_p;
  if (Array.isArray(body.stop_sequences) && body.stop_sequences.length > 0) out.stop = body.stop_sequences;
  if (Array.isArray(body.tools)) {
    const tools = body.tools.flatMap((raw) => {
      if (!isRecord(raw) || typeof raw.name !== 'string') return [];
      return [{ type: 'function', function: { name: raw.name, description: asString(raw.description), parameters: isRecord(raw.input_schema) ? raw.input_schema : { type: 'object', properties: {} } } }];
    });
    if (tools.length > 0) out.tools = tools;
  }
  const choice = body.tool_choice;
  if (isRecord(choice)) {
    if (choice.type === 'auto') out.tool_choice = 'auto';
    else if (choice.type === 'any') out.tool_choice = 'required';
    else if (choice.type === 'tool' && typeof choice.name === 'string') out.tool_choice = { type: 'function', function: { name: choice.name } };
  }
  if (body.stream === true) {
    out.stream = true;
    out.stream_options = { include_usage: true };
  }
  return out;
}

interface GeminiBody {
  contents: Json[];
  systemInstruction?: Json;
  tools?: Json[];
  generationConfig: Json;
}

/** Shared Gemini request shape from either client wire. */
function toGemini(clientWire: 'openai' | 'anthropic', body: Json): GeminiBody {
  const contents: Json[] = [];
  const toolNames = new Map<string, string>();
  let systemText = '';
  let maxTokens: number | null = null;
  let stops: string[] = [];
  let jsonMode = false;
  let temperature: number | null = null;
  let topP: number | null = null;
  const tools: Json[] = [];

  if (clientWire === 'openai') {
    const source = Array.isArray(body.messages) ? body.messages : [];
    for (const raw of source) {
      if (!isRecord(raw)) continue;
      if (raw.role === 'system') {
        const text = textOf(raw.content);
        if (text !== '') systemText = systemText === '' ? text : `${systemText}\n${text}`;
        continue;
      }
      if (raw.role === 'assistant') {
        const parts: Json[] = [];
        const text = textOf(raw.content);
        if (text !== '') parts.push({ text });
        for (const call of Array.isArray(raw.tool_calls) ? raw.tool_calls : []) {
          if (!isRecord(call) || !isRecord(call.function)) continue;
          const name = asString(call.function.name);
          if (asString(call.id) !== '') toolNames.set(asString(call.id), name);
          parts.push({ functionCall: { name, args: argsObject(call.function.arguments) } });
        }
        if (parts.length > 0) contents.push({ role: 'model', parts });
        continue;
      }
      if (raw.role === 'tool') {
        const name = toolNames.get(asString(raw.tool_call_id)) ?? '';
        contents.push({ role: 'user', parts: [{ functionResponse: { name, response: { result: typeof raw.content === 'string' ? raw.content : textOf(raw.content) } } }] });
        continue;
      }
      if (raw.role !== 'user') continue;
      const parts: Json[] = [];
      const content = raw.content;
      if (typeof content === 'string') {
        if (content !== '') parts.push({ text: content });
      } else if (Array.isArray(content)) {
        for (const part of content) {
          if (!isRecord(part)) continue;
          if (part.type === 'text' && typeof part.text === 'string') parts.push({ text: part.text });
          else if (part.type === 'image_url' && isRecord(part.image_url) && typeof part.image_url.url === 'string') {
            const img = imagePartsOf(part.image_url.url);
            if (img !== null) parts.push(img.data !== '' ? { inlineData: { mimeType: img.mime, data: img.data } } : { fileData: { mimeType: 'image/png', fileUri: img.url } });
          }
        }
      }
      if (parts.length > 0) contents.push({ role: 'user', parts });
    }
    if (Array.isArray(body.tools)) {
      for (const raw of body.tools) {
        if (!isRecord(raw) || !isRecord(raw.function) || typeof raw.function.name !== 'string') continue;
        tools.push({ name: raw.function.name, description: asString(raw.function.description), parameters: isRecord(raw.function.parameters) ? raw.function.parameters : { type: 'object', properties: {} } });
      }
    }
    maxTokens = asNumber(body.max_tokens) ?? asNumber(body.max_completion_tokens);
    if (Array.isArray(body.stop)) stops = body.stop.filter((s) => typeof s === 'string');
    jsonMode = isRecord(body.response_format) && (body.response_format.type === 'json_object' || body.response_format.type === 'json_schema');
    temperature = asNumber(body.temperature);
    topP = asNumber(body.top_p);
  } else {
    systemText = textOf(body.system);
    const source = Array.isArray(body.messages) ? body.messages : [];
    for (const raw of source) {
      if (!isRecord(raw)) continue;
      const assistant = raw.role === 'assistant';
      const content = raw.content;
      if (typeof content === 'string') {
        if (content !== '') contents.push({ role: assistant ? 'model' : 'user', parts: [{ text: content }] });
        continue;
      }
      if (!Array.isArray(content)) continue;
      if (assistant) {
        const parts: Json[] = [];
        const text = textOf(content);
        if (text !== '') parts.push({ text });
        for (const block of content) {
          if (!isRecord(block) || block.type !== 'tool_use') continue;
          const name = asString(block.name);
          if (asString(block.id) !== '') toolNames.set(asString(block.id), name);
          parts.push({ functionCall: { name, args: isRecord(block.input) ? block.input : {} } });
        }
        if (parts.length > 0) contents.push({ role: 'model', parts });
        continue;
      }
      const parts: Json[] = [];
      for (const block of content) {
        if (!isRecord(block)) continue;
        if (block.type === 'text' && typeof block.text === 'string') parts.push({ text: block.text });
        else if (block.type === 'image' && isRecord(block.source)) {
          const src = block.source;
          if (src.type === 'base64' && typeof src.data === 'string') parts.push({ inlineData: { mimeType: typeof src.media_type === 'string' ? src.media_type : 'image/png', data: src.data } });
          else if (src.type === 'url' && typeof src.url === 'string') parts.push({ fileData: { mimeType: 'image/png', fileUri: src.url } });
        } else if (block.type === 'tool_result') {
          const name = toolNames.get(asString(block.tool_use_id)) ?? '';
          parts.push({ functionResponse: { name, response: { result: typeof block.content === 'string' ? block.content : textOf(block.content) } } });
        }
      }
      if (parts.length > 0) contents.push({ role: 'user', parts });
    }
    if (Array.isArray(body.tools)) {
      for (const raw of body.tools) {
        if (!isRecord(raw) || typeof raw.name !== 'string') continue;
        tools.push({ name: raw.name, description: asString(raw.description), parameters: isRecord(raw.input_schema) ? raw.input_schema : { type: 'object', properties: {} } });
      }
    }
    maxTokens = asNumber(body.max_tokens);
    if (Array.isArray(body.stop_sequences)) stops = body.stop_sequences.filter((s) => typeof s === 'string');
    temperature = asNumber(body.temperature);
    topP = asNumber(body.top_p);
  }

  const generationConfig: Json = {};
  if (maxTokens !== null) generationConfig.maxOutputTokens = maxTokens;
  if (stops.length > 0) generationConfig.stopSequences = stops;
  if (jsonMode) generationConfig.responseMimeType = 'application/json';
  if (temperature !== null) generationConfig.temperature = temperature;
  if (topP !== null) generationConfig.topP = topP;
  const out: GeminiBody = { contents, generationConfig };
  if (systemText !== '') out.systemInstruction = { parts: [{ text: systemText }] };
  if (tools.length > 0) out.tools = [{ functionDeclarations: tools }];
  return out;
}

// ---------------------------------------------------------------------------
// Wire translation: answers. Every upstream answer is normalised, then
// rendered in the client's wire.
// ---------------------------------------------------------------------------

interface ToolCall {
  id: string;
  name: string;
  args: string;
}

interface Answer {
  text: string;
  inputTokens: number | null;
  outputTokens: number | null;
  stop: 'stop' | 'length' | 'tool_calls' | 'content_filter';
  toolCalls: ToolCall[];
}

function sseFrames(text: string): { event: string | null; data: string }[] {
  const frames: { event: string | null; data: string }[] = [];
  const normalized = text.replace(/\r\n/g, '\n');
  for (const frame of normalized.split('\n\n')) {
    if (frame.trim() === '') continue;
    let event: string | null = null;
    const data: string[] = [];
    for (const line of frame.split('\n')) {
      if (line.startsWith('event:')) event = line.slice('event:'.length).trim();
      else if (line.startsWith('data:')) data.push(line.slice('data:'.length).replace(/^ /, ''));
    }
    if (data.length > 0) frames.push({ event, data: data.join('\n') });
  }
  return frames;
}

function mapStopToOpenAI(stop: string | null): Answer['stop'] {
  if (stop === 'length' || stop === 'max_tokens' || stop === 'MAX_TOKENS') return 'length';
  if (stop === 'tool_calls' || stop === 'function_call' || stop === 'tool_use') return 'tool_calls';
  if (stop === 'content_filter' || stop === 'refusal' || stop === 'SAFETY') return 'content_filter';
  return 'stop';
}

function mapStopToAnthropic(stop: string | null): 'end_turn' | 'max_tokens' | 'tool_use' | 'refusal' {
  if (stop === 'length' || stop === 'max_tokens' || stop === 'MAX_TOKENS') return 'max_tokens';
  if (stop === 'tool_calls' || stop === 'function_call' || stop === 'tool_use') return 'tool_use';
  if (stop === 'content_filter' || stop === 'refusal' || stop === 'SAFETY') return 'refusal';
  return 'end_turn';
}

function parseOpenAIAnswer(bodyText: string, stream: boolean): Answer | null {
  if (!stream) {
    let value: unknown;
    try {
      value = JSON.parse(bodyText);
    } catch {
      return null;
    }
    if (!isRecord(value)) return null;
    const choices = Array.isArray(value.choices) ? value.choices : [];
    const choice = isRecord(choices[0]) ? choices[0] : {};
    const message = isRecord(choice.message) ? choice.message : {};
    const usage = isRecord(value.usage) ? value.usage : {};
    const toolCalls: ToolCall[] = [];
    for (const call of Array.isArray(message.tool_calls) ? message.tool_calls : []) {
      if (!isRecord(call) || !isRecord(call.function)) continue;
      toolCalls.push({ id: asString(call.id), name: asString(call.function.name), args: typeof call.function.arguments === 'string' ? call.function.arguments : JSON.stringify(call.function.arguments ?? {}) });
    }
    return {
      text: typeof message.content === 'string' ? message.content : textOf(message.content),
      inputTokens: asNumber(usage.prompt_tokens),
      outputTokens: asNumber(usage.completion_tokens),
      stop: mapStopToOpenAI(typeof choice.finish_reason === 'string' ? choice.finish_reason : null),
      toolCalls,
    };
  }
  let text = '';
  let inputTokens: number | null = null;
  let outputTokens: number | null = null;
  let finish: string | null = null;
  const toolCalls: ToolCall[] = [];
  for (const frame of sseFrames(bodyText)) {
    const payload = frame.data.trim();
    if (payload === '[DONE]') continue;
    let chunk: unknown;
    try {
      chunk = JSON.parse(payload);
    } catch {
      continue;
    }
    if (!isRecord(chunk)) continue;
    if (isRecord(chunk.usage)) {
      inputTokens = asNumber(chunk.usage.prompt_tokens) ?? inputTokens;
      outputTokens = asNumber(chunk.usage.completion_tokens) ?? outputTokens;
    }
    const choices = Array.isArray(chunk.choices) ? chunk.choices : [];
    const choice = isRecord(choices[0]) ? choices[0] : null;
    if (choice === null) continue;
    const delta = isRecord(choice.delta) ? choice.delta : {};
    if (typeof delta.content === 'string') text += delta.content;
    for (const call of Array.isArray(delta.tool_calls) ? delta.tool_calls : []) {
      if (!isRecord(call)) continue;
      const idx = typeof call.index === 'number' ? call.index : toolCalls.length;
      while (toolCalls.length <= idx) toolCalls.push({ id: '', name: '', args: '' });
      const fn = isRecord(call.function) ? call.function : {};
      if (typeof call.id === 'string' && call.id !== '') toolCalls[idx].id = call.id;
      if (typeof fn.name === 'string' && fn.name !== '') toolCalls[idx].name = fn.name;
      if (typeof fn.arguments === 'string') toolCalls[idx].args += fn.arguments;
    }
    if (typeof choice.finish_reason === 'string') finish = choice.finish_reason;
  }
  return { text, inputTokens, outputTokens, stop: mapStopToOpenAI(finish), toolCalls };
}

function parseAnthropicAnswer(bodyText: string, stream: boolean): Answer | null {
  if (!stream) {
    let value: unknown;
    try {
      value = JSON.parse(bodyText);
    } catch {
      return null;
    }
    if (!isRecord(value) || !Array.isArray(value.content)) return null;
    const usage = isRecord(value.usage) ? value.usage : {};
    const toolCalls: ToolCall[] = [];
    let text = '';
    for (const block of value.content) {
      if (!isRecord(block)) continue;
      if (block.type === 'text' && typeof block.text === 'string') text += block.text;
      else if (block.type === 'tool_use') toolCalls.push({ id: asString(block.id), name: asString(block.name), args: JSON.stringify(isRecord(block.input) ? block.input : {}) });
    }
    return {
      text,
      inputTokens: asNumber(usage.input_tokens),
      outputTokens: asNumber(usage.output_tokens),
      stop: mapStopToOpenAI(typeof value.stop_reason === 'string' ? value.stop_reason : null),
      toolCalls,
    };
  }
  let text = '';
  let inputTokens: number | null = null;
  let outputTokens: number | null = null;
  let stop: string | null = null;
  const toolCalls: ToolCall[] = [];
  const toolByIndex = new Map<number, number>();
  for (const frame of sseFrames(bodyText)) {
    let data: unknown;
    try {
      data = JSON.parse(frame.data);
    } catch {
      continue;
    }
    if (!isRecord(data)) continue;
    if (frame.event === 'message_start' && isRecord(data.message) && isRecord(data.message.usage)) inputTokens = asNumber(data.message.usage.input_tokens) ?? inputTokens;
    if (frame.event === 'content_block_start' && isRecord(data.content_block) && data.content_block.type === 'tool_use') {
      const index = asNumber(data.index) ?? 0;
      const slot = toolCalls.length;
      toolByIndex.set(index, slot);
      toolCalls.push({ id: asString(data.content_block.id), name: asString(data.content_block.name), args: '' });
    }
    if (frame.event === 'content_block_delta' && isRecord(data.delta)) {
      const index = asNumber(data.index) ?? 0;
      if (data.delta.type === 'text_delta' && typeof data.delta.text === 'string') text += data.delta.text;
      else if (data.delta.type === 'input_json_delta' && typeof data.delta.partial_json === 'string') {
        const slot = toolByIndex.get(index);
        if (slot !== undefined) toolCalls[slot].args += data.delta.partial_json;
      }
    }
    if (frame.event === 'message_delta' && isRecord(data.delta) && typeof data.delta.stop_reason === 'string') stop = data.delta.stop_reason;
    if (frame.event === 'message_delta' && isRecord(data.usage)) outputTokens = asNumber(data.usage.output_tokens) ?? outputTokens;
  }
  return { text, inputTokens, outputTokens, stop: mapStopToOpenAI(stop), toolCalls };
}

function parseGeminiAnswer(bodyText: string, stream: boolean): Answer | null {
  if (!stream) {
    let value: unknown;
    try {
      value = JSON.parse(bodyText);
    } catch {
      return null;
    }
    if (!isRecord(value) || !Array.isArray(value.candidates) || value.candidates.length === 0) return null;
    const candidate = isRecord(value.candidates[0]) ? value.candidates[0] : {};
    const content = isRecord(candidate.content) ? candidate.content : {};
    const parts = Array.isArray(content.parts) ? content.parts : [];
    const text = parts.flatMap((p) => (isRecord(p) && typeof p.text === 'string' ? [p.text] : [])).join('');
    const meta = isRecord(value.usageMetadata) ? value.usageMetadata : {};
    return {
      text,
      inputTokens: asNumber(meta.promptTokenCount),
      outputTokens: asNumber(meta.candidatesTokenCount),
      stop: mapStopToOpenAI(typeof candidate.finishReason === 'string' ? candidate.finishReason : null),
      toolCalls: [],
    };
  }
  let text = '';
  let inputTokens: number | null = null;
  let outputTokens: number | null = null;
  let finish: string | null = null;
  for (const frame of sseFrames(bodyText)) {
    let value: unknown;
    try {
      value = JSON.parse(frame.data);
    } catch {
      continue;
    }
    if (!isRecord(value)) continue;
    const candidates = Array.isArray(value.candidates) ? value.candidates : [];
    const candidate = isRecord(candidates[0]) ? candidates[0] : {};
    const content = isRecord(candidate.content) ? candidate.content : {};
    const parts = Array.isArray(content.parts) ? content.parts : [];
    for (const p of parts) if (isRecord(p) && typeof p.text === 'string') text += p.text;
    if (typeof candidate.finishReason === 'string') finish = candidate.finishReason;
    if (isRecord(value.usageMetadata)) {
      inputTokens = asNumber(value.usageMetadata.promptTokenCount) ?? inputTokens;
      outputTokens = asNumber(value.usageMetadata.candidatesTokenCount) ?? outputTokens;
    }
  }
  return { text, inputTokens, outputTokens, stop: mapStopToOpenAI(finish), toolCalls: [] };
}

function parseAnswer(wire: Wire, bodyText: string, stream: boolean): Answer | null {
  if (wire === 'openai') return parseOpenAIAnswer(bodyText, stream);
  if (wire === 'anthropic') return parseAnthropicAnswer(bodyText, stream);
  return parseGeminiAnswer(bodyText, stream);
}

/** The provider's own usage formula, as a fallback when an answer carries none. */
function fallbackUsage(clientWire: 'openai' | 'anthropic', upstreamWire: Wire, upstreamBody: Json, text: string): { input: number; output: number } {
  let systemText = '';
  let userText = '';
  let tools: string[] = [];
  if (upstreamWire === 'openai') {
    const source = Array.isArray(upstreamBody.messages) ? upstreamBody.messages : [];
    const systemParts: string[] = [];
    for (const m of source) {
      if (!isRecord(m)) continue;
      if (m.role === 'system') {
        const t = textOf(m.content);
        if (t !== '') systemParts.push(t);
      } else if (m.role === 'user') userText = textOf(m.content);
    }
    systemText = systemParts.join('\n');
    if (Array.isArray(upstreamBody.tools)) {
      for (const t of upstreamBody.tools) if (isRecord(t) && isRecord(t.function) && typeof t.function.name === 'string') tools.push(t.function.name);
    }
  } else if (upstreamWire === 'anthropic') {
    systemText = textOf(upstreamBody.system);
    const source = Array.isArray(upstreamBody.messages) ? upstreamBody.messages : [];
    for (const m of source) {
      if (!isRecord(m) || m.role !== 'user') continue;
      userText = textOf(m.content);
    }
    if (Array.isArray(upstreamBody.tools)) {
      for (const t of upstreamBody.tools) if (isRecord(t) && typeof t.name === 'string') tools.push(t.name);
    }
  } else {
    const g = upstreamBody as unknown as GeminiBody;
    if (isRecord(g.systemInstruction)) systemText = textOf((g.systemInstruction.parts ?? []).flatMap((p) => (isRecord(p) && typeof p.text === 'string' ? [p.text] : [])));
    for (const c of g.contents ?? []) {
      if (!isRecord(c) || c.role === 'model') continue;
      userText = (Array.isArray(c.parts) ? c.parts : []).flatMap((p) => (isRecord(p) && typeof p.text === 'string' ? [p.text] : [])).join('');
    }
    for (const t of g.tools ?? []) {
      if (!isRecord(t) || !Array.isArray(t.functionDeclarations)) continue;
      for (const fn of t.functionDeclarations) if (isRecord(fn) && typeof fn.name === 'string') tools.push(fn.name);
    }
  }
  void clientWire;
  const prompt = systemText.length + userText.length + tools.join(',').length;
  return { input: 1 + Math.floor(prompt / 4), output: 1 + Math.floor(text.length / 4) };
}

function sseEvent(event: string | null, data: unknown): string {
  return `${event === null ? '' : `event: ${event}\n`}data: ${JSON.stringify(data)}\n\n`;
}

function splitPieces(text: string): string[] {
  const out: string[] = [];
  for (let at = 0; at < text.length; at += 13) out.push(text.slice(at, at + 13));
  return out.length === 0 ? [''] : out;
}

function renderOpenAI(ans: Answer, model: string, stream: boolean): { status: number; contentType: string; body: string } {
  const input = ans.inputTokens ?? 0;
  const output = ans.outputTokens ?? 0;
  if (!stream) {
    const message: Json = { role: 'assistant', content: ans.text };
    if (ans.toolCalls.length > 0) {
      message.tool_calls = ans.toolCalls.map((c) => ({ id: c.id !== '' ? c.id : 'call_unknown', type: 'function', function: { name: c.name, arguments: c.args } }));
    }
    return {
      status: 200,
      contentType: 'application/json',
      body: JSON.stringify({
        id: 'chatcmpl-agg',
        object: 'chat.completion',
        created: FIXED_CREATED,
        model,
        choices: [{ index: 0, message, finish_reason: ans.stop, logprobs: null }],
        usage: { prompt_tokens: input, completion_tokens: output, total_tokens: input + output },
      }),
    };
  }
  const chunk = (delta: Json, finish: string | null): Json => ({
    id: 'chatcmpl-agg',
    object: 'chat.completion.chunk',
    created: FIXED_CREATED,
    model,
    choices: [{ index: 0, delta, finish_reason: finish }],
  });
  let out = sseEvent(null, chunk({ role: 'assistant', content: '' }, null));
  for (const piece of splitPieces(ans.text)) out += sseEvent(null, chunk({ content: piece }, null));
  for (const call of ans.toolCalls) {
    out += sseEvent(null, chunk({ tool_calls: [{ index: 0, id: call.id, type: 'function', function: { name: call.name, arguments: call.args } }] }, null));
  }
  out += sseEvent(null, chunk({}, ans.stop));
  out += sseEvent(null, { id: 'chatcmpl-agg', object: 'chat.completion.chunk', created: FIXED_CREATED, model, choices: [], usage: { prompt_tokens: input, completion_tokens: output, total_tokens: input + output } });
  out += 'data: [DONE]\n\n';
  return { status: 200, contentType: 'text/event-stream', body: out };
}

function renderAnthropic(ans: Answer, model: string, stream: boolean): { status: number; contentType: string; body: string } {
  const input = ans.inputTokens ?? 0;
  const output = ans.outputTokens ?? 0;
  const stopReason = mapStopToAnthropic(ans.stop);
  if (!stream) {
    const content: Json[] = [];
    if (ans.text !== '') content.push({ type: 'text', text: ans.text });
    for (const call of ans.toolCalls) {
      content.push({ type: 'tool_use', id: call.id !== '' ? call.id : 'toolu_unknown', name: call.name, input: argsObject(call.args) });
    }
    if (content.length === 0) content.push({ type: 'text', text: '' });
    return {
      status: 200,
      contentType: 'application/json',
      body: JSON.stringify({
        id: 'msg_agg',
        type: 'message',
        role: 'assistant',
        model,
        content,
        stop_reason: stopReason,
        stop_sequence: null,
        usage: { input_tokens: input, output_tokens: output },
      }),
    };
  }
  let out = sseEvent('message_start', {
    type: 'message_start',
    message: { id: 'msg_agg', type: 'message', role: 'assistant', model, content: [], stop_reason: null, stop_sequence: null, usage: { input_tokens: input, output_tokens: 0 } },
  });
  let index = 0;
  if (ans.text !== '') {
    out += sseEvent('content_block_start', { type: 'content_block_start', index, content_block: { type: 'text', text: '' } });
    for (const piece of splitPieces(ans.text)) out += sseEvent('content_block_delta', { type: 'content_block_delta', index, delta: { type: 'text_delta', text: piece } });
    out += sseEvent('content_block_stop', { type: 'content_block_stop', index });
    index += 1;
  }
  for (const call of ans.toolCalls) {
    out += sseEvent('content_block_start', { type: 'content_block_start', index, content_block: { type: 'tool_use', id: call.id !== '' ? call.id : `toolu_${index}`, name: call.name, input: {} } });
    if (call.args !== '') out += sseEvent('content_block_delta', { type: 'content_block_delta', index, delta: { type: 'input_json_delta', partial_json: call.args } });
    out += sseEvent('content_block_stop', { type: 'content_block_stop', index });
    index += 1;
  }
  out += sseEvent('message_delta', { type: 'message_delta', delta: { stop_reason: stopReason, stop_sequence: null }, usage: { output_tokens: output } });
  out += sseEvent('message_stop', { type: 'message_stop' });
  return { status: 200, contentType: 'text/event-stream', body: out };
}

// ---------------------------------------------------------------------------
// Upstream calls.
// ---------------------------------------------------------------------------

interface UpstreamResult {
  ok: boolean;
  status: number;
  answer: Answer | null;
}

async function callUpstream(candidate: Candidate, clientWire: 'openai' | 'anthropic', body: Json, stream: boolean): Promise<UpstreamResult> {
  const provider = candidate.probe.provider;
  const base = joinBase(ENV_URL, provider.base);
  const headers: Record<string, string> = { 'content-type': 'application/json', ...authHeaders(provider.auth, provider.key) };
  if (stream) headers.accept = 'text/event-stream';
  let url: string;
  let payload: Json;
  if (provider.wire === 'openai') {
    url = `${base}/v1/chat/completions`;
    payload = clientWire === 'openai' ? { ...body, model: candidate.model.id } : anthropicToOpenAI(body, candidate.model.id);
    if (stream) payload.stream_options = { include_usage: true };
  } else if (provider.wire === 'anthropic') {
    url = `${base}/v1/messages`;
    headers['anthropic-version'] = ANTHROPIC_VERSION;
    payload = clientWire === 'anthropic' ? { ...body, model: candidate.model.id } : openAIToAnthropic(body, candidate.model.id);
  } else {
    const gemini = toGemini(clientWire, body);
    url = `${base}/v1beta/models/${encodeURIComponent(candidate.model.id)}:${stream ? 'streamGenerateContent' : 'generateContent'}`;
    payload = gemini as unknown as Json;
  }
  let res: Response;
  try {
    res = await fetch(url, { method: 'POST', headers, body: JSON.stringify(payload), signal: AbortSignal.timeout(UPSTREAM_TIMEOUT_MS) });
  } catch {
    return { ok: false, status: 0, answer: null };
  }
  const text = await res.text();
  if (!res.ok) return { ok: false, status: res.status, answer: null };
  const answer = parseAnswer(provider.wire, text, stream);
  if (answer === null) return { ok: false, status: 502, answer: null };
  if (answer.inputTokens === null || answer.outputTokens === null) {
    const fallback = fallbackUsage(clientWire, provider.wire, payload, answer.text);
    if (answer.inputTokens === null) answer.inputTokens = fallback.input;
    if (answer.outputTokens === null) answer.outputTokens = fallback.output;
  }
  return { ok: true, status: res.status, answer };
}

// ---------------------------------------------------------------------------
// The server.
// ---------------------------------------------------------------------------

const brief = readBrief(PROMPT_FILE);
let probes: Probe[] = [];
let catalog: Catalog = { providers: [] };
let manifestIndex = 0;

async function load(): Promise<void> {
  if (brief.error !== null) log(brief.error);
  probes = await Promise.all(brief.providers.map((p) => probeProvider(p)));
  for (const probe of probes) if (!probe.ok && probe.error !== null) log(probe.error);
  catalog = buildCatalog(brief, probes);
}

function servedModels(): { id: string; provider: string; displayName: string }[] {
  const seen = new Set<string>();
  const out: { id: string; provider: string; displayName: string }[] = [];
  for (const probe of probes) {
    if (!probe.ok) continue;
    for (const model of [...probe.models].sort((a, b) => (a.id < b.id ? -1 : a.id > b.id ? 1 : 0))) {
      const row = ratingRow(brief, probe.provider.id, model.id);
      if (!modelIsFree(model, row)) continue;
      if (seen.has(model.id)) continue;
      seen.add(model.id);
      out.push({ id: model.id, provider: probe.provider.id, displayName: model.displayName });
    }
  }
  return out;
}

function requirementsFor(body: Json): Requirement {
  const entry = brief.manifest[manifestIndex];
  manifestIndex += 1;
  if (entry !== undefined) return entry.requires;
  const messages = Array.isArray(body.messages) ? body.messages : [];
  let vision = false;
  for (const message of messages) {
    if (!isRecord(message) || !Array.isArray(message.content)) continue;
    for (const block of message.content) {
      if (isRecord(block) && (block.type === 'image' || block.type === 'image_url')) vision = true;
    }
  }
  const format = isRecord(body.response_format) ? body.response_format.type : undefined;
  return { tools: Array.isArray(body.tools) && body.tools.length > 0, vision, json: format === 'json_object' || format === 'json_schema' };
}

async function readJson(req: IncomingMessage): Promise<Json | null> {
  const chunks: Buffer[] = [];
  let size = 0;
  for await (const chunk of req) {
    const buffer = Buffer.isBuffer(chunk) ? chunk : Buffer.from(String(chunk));
    size += buffer.length;
    if (size > MAX_BODY_BYTES) return null;
    chunks.push(buffer);
  }
  if (chunks.length === 0) return null;
  try {
    const value: unknown = JSON.parse(Buffer.concat(chunks).toString('utf8'));
    return isRecord(value) ? value : null;
  } catch {
    return null;
  }
}

function sendJson(res: ServerResponse, status: number, body: unknown): void {
  const text = JSON.stringify(body);
  res.writeHead(status, { 'content-type': 'application/json', 'content-length': Buffer.byteLength(text) });
  res.end(text);
}

function sendError(res: ServerResponse, wire: 'openai' | 'anthropic', status: number, type: string, message: string): void {
  if (wire === 'anthropic') sendJson(res, status, { type: 'error', error: { type, message } });
  else sendJson(res, status, { error: { message, type, code: null } });
}

function modelsRoute(req: IncomingMessage, res: ServerResponse): void {
  const list = servedModels();
  if (typeof req.headers['x-api-key'] === 'string') {
    sendJson(res, 200, { data: list.map((m) => ({ type: 'model', id: m.id, display_name: m.displayName, created_at: FIXED_CREATED_AT })), has_more: false });
  } else {
    sendJson(res, 200, { object: 'list', data: list.map((m) => ({ id: m.id, object: 'model', created: FIXED_CREATED, owned_by: m.provider })) });
  }
}

async function serveRequest(clientWire: 'openai' | 'anthropic', body: Json, stream: boolean): Promise<{ status: number; contentType: string; body: string } | null> {
  const requires = requirementsFor(body);
  const candidates = candidatesFor(brief, probes, requires);
  const tried = new Set<string>();
  for (const candidate of candidates) {
    const state = stateOf(candidate.probe.provider.id);
    if (state.benched && candidates.some((c) => !stateOf(c.probe.provider.id).benched && !tried.has(`${c.probe.provider.id}/${c.model.id}`))) continue;
    const key = `${candidate.probe.provider.id}/${candidate.model.id}`;
    if (tried.has(key)) continue;
    tried.add(key);
    state.calls += 1;
    const result = await callUpstream(candidate, clientWire, body, stream);
    if (result.ok && result.answer !== null) {
      state.consecFail = 0;
      const rendered = clientWire === 'openai' ? renderOpenAI(result.answer, candidate.model.id, stream) : renderAnthropic(result.answer, candidate.model.id, stream);
      return rendered;
    }
    // A refusal: bench at once on 402 (budget spent), else after two in a row.
    if (result.status === 402) state.benched = true;
    else {
      state.consecFail += 1;
      if (state.consecFail >= 2) state.benched = true;
    }
  }
  return null;
}

async function handle(req: IncomingMessage, res: ServerResponse): Promise<void> {
  const url = new URL(req.url ?? '/', 'http://entry');
  if (req.method === 'GET' && url.pathname === '/v1/models') {
    modelsRoute(req, res);
    return;
  }
  if (req.method === 'POST' && (url.pathname === '/v1/chat/completions' || url.pathname === '/v1/messages')) {
    const wire: 'openai' | 'anthropic' = url.pathname === '/v1/messages' ? 'anthropic' : 'openai';
    const body = await readJson(req);
    if (body === null) return sendError(res, wire, 400, 'invalid_request_error', 'the request body is not one JSON object');
    const stream = body.stream === true;
    const rendered = await serveRequest(wire, body, stream);
    if (rendered === null) return sendError(res, wire, 503, wire === 'anthropic' ? 'overloaded_error' : 'no_free_model', 'no free model on the directory meets this request');
    res.writeHead(rendered.status, { 'content-type': rendered.contentType, 'content-length': Buffer.byteLength(rendered.body) });
    res.end(rendered.body);
    return;
  }
  sendError(res, url.pathname === '/v1/messages' ? 'anthropic' : 'openai', 404, 'not_found', `no route ${req.method ?? ''} ${url.pathname}`);
}

function listen(server: Server): Promise<void> {
  return new Promise((resolve, reject) => {
    server.once('error', reject);
    server.listen(0, '0.0.0.0', () => resolve());
  });
}

async function register(port: number): Promise<void> {
  let lastError: string | null = null;
  for (let attempt = 0; attempt < 3; attempt++) {
    try {
      const res = await fetch(`${ENV_URL}/register`, {
        method: 'POST',
        headers: { 'content-type': 'application/json' },
        body: JSON.stringify({ protocol_version: PROTOCOL_VERSION, port, catalog }),
        signal: AbortSignal.timeout(5000),
      });
      if (!res.ok) throw new Error(`POST ${ENV_URL}/register answered HTTP ${res.status}: ${(await res.text()).slice(0, 500)}`);
      const reply: unknown = await res.json();
      if (isRecord(reply) && typeof reply.requests === 'number') log(`registered: the workload holds ${reply.requests} requests`);
      return;
    } catch (err) {
      lastError = (err as Error).message;
      await new Promise((resolve) => setTimeout(resolve, 500));
    }
  }
  throw new Error(`register failed: ${lastError}`);
}

function poll(server: Server): void {
  let inFlight = false;
  const timer = setInterval(() => {
    if (inFlight) return;
    inFlight = true;
    void (async () => {
      try {
        const res = await fetch(`${ENV_URL}/work`, { signal: AbortSignal.timeout(2000) });
        if (res.status === 200) {
          const work: unknown = await res.json();
          if (isRecord(work) && work.done === true) {
            clearInterval(timer);
            server.close();
            process.exit(0);
          }
        }
      } catch {
        // The env is not reachable yet: try again on the next tick.
      } finally {
        inFlight = false;
      }
    })();
  }, POLL_MS);
}

async function start(): Promise<void> {
  await load();
  if (process.argv.includes('--list')) {
    const text = `${JSON.stringify(catalog)}\n`;
    await new Promise<void>((resolve) => process.stdout.write(text, () => resolve()));
    process.exit(0);
  }
  if (ENV_URL === '') throw new Error('MAKEFUN_ENV_URL is not set');
  const server = createServer((req, res) => {
    void handle(req, res).catch((err: unknown) => {
      log((err as Error).stack ?? String(err));
      if (!res.headersSent) sendError(res, req.url?.startsWith('/v1/messages') === true ? 'anthropic' : 'openai', 500, 'internal_error', 'the server failed to answer');
    });
  });
  await listen(server);
  const address = server.address();
  if (address === null || typeof address === 'string') throw new Error('the server has no port');
  const port = address.port;
  if (port === 8080) throw new Error('the OS picked 8080, which the env owns on one layout: restart the trial');
  log(`listening on 0.0.0.0:${port} with ${catalog.providers.length} answered providers and ${servedModels().length} free models`);
  await register(port);
  process.on('SIGTERM', () => process.exit(0));
  process.on('SIGINT', () => process.exit(0));
  poll(server);
}

try {
  await start();
} catch (err) {
  log((err as Error).stack ?? String(err));
  process.exit(1);
}
