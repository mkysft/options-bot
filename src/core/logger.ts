import { inspect } from "node:util";

const LEVELS = ["debug", "info", "warn", "error"] as const;
type Level = (typeof LEVELS)[number];
type LogFormat = "pretty" | "json";

const parseLevel = (value: string | undefined): Level => {
  const normalized = String(value ?? "info").toLowerCase();
  return LEVELS.includes(normalized as Level) ? (normalized as Level) : "info";
};

const parseFormat = (value: string | undefined): LogFormat => {
  const normalized = String(value ?? "").toLowerCase();
  if (normalized === "json") return "json";
  if (normalized === "pretty") return "pretty";
  return process.env.APP_ENV === "prod" ? "json" : "pretty";
};

const shouldLog = (messageLevel: Level, configured: Level): boolean =>
  LEVELS.indexOf(messageLevel) >= LEVELS.indexOf(configured);

const configuredLevel = parseLevel(process.env.LOG_LEVEL);
const configuredFormat = parseFormat(process.env.LOG_FORMAT);

const supportColor =
  Boolean(process.stdout.isTTY) &&
  process.env.NO_COLOR === undefined &&
  process.env.TERM !== "dumb";

const levelColor: Record<Level, number> = {
  debug: 90,
  info: 36,
  warn: 33,
  error: 31
};

const colorize = (text: string, colorCode: number): string =>
  supportColor ? `\u001b[${colorCode}m${text}\u001b[0m` : text;

const stamp = (): string => new Date().toISOString();

const toJsonSafe = (value: unknown, seen = new WeakSet<object>()): unknown => {
  if (value instanceof Error) {
    return {
      name: value.name,
      message: value.message,
      stack: value.stack,
      cause: toJsonSafe(value.cause, seen)
    };
  }
  if (typeof value === "bigint") return value.toString();
  if (typeof value === "function") return `[Function ${value.name || "anonymous"}]`;
  if (!value || typeof value !== "object") return value;
  if (seen.has(value as object)) return "[Circular]";
  seen.add(value as object);

  if (Array.isArray(value)) {
    return value.map((item) => toJsonSafe(item, seen));
  }

  const out: Record<string, unknown> = {};
  for (const [key, item] of Object.entries(value as Record<string, unknown>)) {
    out[key] = toJsonSafe(item, seen);
  }
  return out;
};

const inspectMeta = (value: unknown): string =>
  inspect(value, {
    colors: supportColor,
    depth: 8,
    compact: false,
    breakLength: 110,
    maxArrayLength: 50
  });

const splitMessageAndMeta = (args: unknown[]): { message: string; meta: unknown[] } => {
  if (args.length === 0) return { message: "", meta: [] };
  if (typeof args[0] === "string") {
    return { message: args[0], meta: args.slice(1) };
  }
  return { message: "log", meta: args };
};

const writePretty = (level: Level, args: unknown[]): void => {
  const { message, meta } = splitMessageAndMeta(args);
  const levelLabel = colorize(level.toUpperCase().padEnd(5), levelColor[level]);
  const appName = process.env.APP_NAME ?? "options-bot";
  const header = `${stamp()} ${levelLabel} [${appName}] ${message}`;
  const sink = level === "warn" || level === "error" ? process.stderr : process.stdout;
  if (meta.length === 0) {
    sink.write(`${header}\n`);
    return;
  }

  const lines = meta.map((entry, index) => {
    const branch = index === meta.length - 1 ? "└─" : "├─";
    return `  ${branch} ${inspectMeta(entry)}`;
  });
  sink.write(`${header}\n${lines.join("\n")}\n`);
};

const writeJson = (level: Level, args: unknown[]): void => {
  const { message, meta } = splitMessageAndMeta(args);
  const payload = {
    timestamp: stamp(),
    level,
    app: process.env.APP_NAME ?? "options-bot",
    message,
    meta: meta.length > 0 ? toJsonSafe(meta) : undefined
  };
  const sink = level === "warn" || level === "error" ? process.stderr : process.stdout;
  sink.write(`${JSON.stringify(payload)}\n`);
};

const writeLog = (level: Level, args: unknown[]): void => {
  if (!shouldLog(level, configuredLevel)) return;
  if (configuredFormat === "json") writeJson(level, args);
  else writePretty(level, args);
};

export const logger = {
  debug: (...args: unknown[]) => writeLog("debug", args),
  info: (...args: unknown[]) => writeLog("info", args),
  warn: (...args: unknown[]) => writeLog("warn", args),
  error: (...args: unknown[]) => writeLog("error", args)
};
