import { createHash } from "node:crypto";

import { TooLarge, TypeError } from "./errors.js";
import { JsonFloat, canonicalValueJson, parseJsonValue } from "./jsonio.js";

export const BLOB_THRESHOLD = 256;
export const MAX_VALUE_BYTES = 1_048_576;
export const INT64_MIN = -(2n ** 63n);
export const INT64_MAX = 2n ** 63n - 1n;
export const INSTANT_MIN = -62_135_596_800_000_000n;
export const INSTANT_MAX = 253_402_300_799_999_999n;
export const ATTRIBUTE_PATTERN =
  /^[a-z0-9][a-z0-9._-]*\/[a-z0-9][a-z0-9._-]*$/u;

export const REF = 0;
export const BOOL = 1;
export const INT = 2;
export const FLOAT = 3;
export const TEXT = 4;
export const INSTANT = 5;
export const BYTES = 6;
export const VECTOR = 7;
export const TEXT_REF = 8;
export const BYTES_REF = 9;
export const JSON_TAG = 10;

export type Stored = bigint | number | string | Buffer;
export interface Encoded {
  tag: number;
  stored: Stored;
  logical: unknown;
  blob?: string | Buffer;
}

export interface Cell {
  tag: number;
  value: unknown;
}

export const TYPE_NAMES = new Set([
  "ref",
  "bool",
  "int",
  "float",
  "text",
  "instant",
  "bytes",
  "vector",
  "json",
]);
const TAG_NAMES = [
  "ref",
  "bool",
  "int",
  "float",
  "text",
  "instant",
  "bytes",
  "vector",
  "text",
  "bytes",
  "json",
] as const;

function utf8(value: string): Buffer {
  if (!value.isWellFormed()) {
    throw new TypeError(
      `string ${JSON.stringify(value)} is not valid UTF-8; remove unpaired surrogate code points`,
    );
  }
  return Buffer.from(value, "utf8");
}

function checkSize(data: Uint8Array, value: unknown): void {
  if (data.byteLength > MAX_VALUE_BYTES) {
    throw new TooLarge(
      `value ${typeof value} is ${data.byteLength} bytes; keep fact values at or below ${MAX_VALUE_BYTES} bytes`,
    );
  }
}

export function indirectDigest(tag: number, data: Uint8Array): Buffer {
  // The physical tag keeps byte-identical values from different domains apart.
  return createHash("sha256")
    .update(Buffer.from([tag]))
    .update(data)
    .digest();
}

function asInt64(value: unknown): bigint {
  if (typeof value === "bigint") {
    if (value >= INT64_MIN && value <= INT64_MAX) return value;
  } else if (typeof value === "number" && Number.isSafeInteger(value)) {
    return BigInt(value);
  }
  throw new TypeError(
    `integer ${String(value)} exceeds signed 64-bit range; store a smaller integer or text`,
  );
}

const RFC3339 =
  /^([0-9]{4})-([0-9]{2})-([0-9]{2})T([0-9]{2}):([0-9]{2}):([0-9]{2})(?:\.([0-9]+))?(Z|[+-][0-9]{2}:[0-9]{2})$/u;

export function instantValue(value: unknown): bigint {
  if (typeof value === "number" || typeof value === "bigint") {
    const instant = asInt64(value);
    if (instant >= INSTANT_MIN && instant <= INSTANT_MAX) return instant;
    throw new TypeError(
      `instant ${String(value)} is outside RFC 3339 years 0001..9999; use representable UTC microseconds`,
    );
  }
  if (typeof value !== "string") {
    throw new TypeError(
      `invalid instant ${String(value)}; use integer microseconds or an RFC 3339 UTC string`,
    );
  }
  const match = RFC3339.exec(value);
  if (match === null)
    throw new TypeError(
      `invalid instant ${JSON.stringify(value)}; use RFC 3339 such as 2026-08-24T10:00:00Z`,
    );
  const [
    ,
    yearText,
    monthText,
    dayText,
    hourText,
    minuteText,
    secondText,
    fraction = "",
    zone,
  ] = match;
  const year = Number(yearText);
  const month = Number(monthText);
  const day = Number(dayText);
  const hour = Number(hourText);
  const minute = Number(minuteText);
  const second = Number(secondText);
  const fractionMicros = BigInt((fraction + "000000").slice(0, 6));
  const base = new Date(0);
  base.setUTCFullYear(year, month - 1, day);
  base.setUTCHours(hour, minute, second, 0);
  if (
    base.getUTCFullYear() !== year ||
    base.getUTCMonth() !== month - 1 ||
    base.getUTCDate() !== day ||
    base.getUTCHours() !== hour ||
    base.getUTCMinutes() !== minute ||
    base.getUTCSeconds() !== second
  ) {
    throw new TypeError(
      `invalid instant ${JSON.stringify(value)}; use RFC 3339 such as 2026-08-24T10:00:00Z`,
    );
  }
  let offsetMinutes = 0;
  if (zone !== "Z") {
    const sign = zone?.[0] === "+" ? 1 : -1;
    const zoneHour = Number(zone?.slice(1, 3));
    const zoneMinute = Number(zone?.slice(4, 6));
    if (zoneHour > 23 || zoneMinute > 59)
      throw new TypeError(
        `invalid instant ${JSON.stringify(value)}; use a valid UTC offset`,
      );
    offsetMinutes = sign * (zoneHour * 60 + zoneMinute);
  }
  const result =
    BigInt(base.getTime() - offsetMinutes * 60_000) * 1000n + fractionMicros;
  if (result < INSTANT_MIN || result > INSTANT_MAX)
    throw new TypeError(
      `instant ${JSON.stringify(value)} is outside RFC 3339 years 0001..9999`,
    );
  return result;
}

function bytesValue(value: unknown): Buffer {
  let result: Buffer;
  if (Buffer.isBuffer(value)) result = Buffer.from(value);
  else if (value instanceof Uint8Array) result = Buffer.from(value);
  else if (typeof value === "string") {
    result = Buffer.from(value, "base64");
    if (result.toString("base64") !== value)
      throw new TypeError(
        `invalid bytes value ${JSON.stringify(value)}; use standard padded base64`,
      );
  } else
    throw new TypeError(
      `invalid bytes value ${String(value)}; use bytes or a standard padded base64 string`,
    );
  checkSize(result, value);
  return result;
}

function vectorValue(value: unknown): { logical: number[]; packed: Buffer } {
  if (!Array.isArray(value) || value.length === 0)
    throw new TypeError(
      `invalid vector ${String(value)}; use a non-empty array of finite numbers`,
    );
  const packed = Buffer.allocUnsafe(value.length * 4);
  const logical: number[] = [];
  for (const [index, item] of value.entries()) {
    if (
      (typeof item !== "number" &&
        typeof item !== "bigint" &&
        !(item instanceof JsonFloat)) ||
      !Number.isFinite(Number(item))
    ) {
      throw new TypeError(
        `invalid vector element ${String(item)}; use only finite numbers`,
      );
    }
    packed.writeFloatLE(Number(item), index * 4);
    const rounded = packed.readFloatLE(index * 4);
    if (!Number.isFinite(rounded))
      throw new TypeError(
        `vector element ${String(item)} cannot be represented as float32; reduce its magnitude`,
      );
    logical.push(rounded);
  }
  checkSize(packed, value);
  return { logical, packed };
}

export function encode(
  value: unknown,
  resolveRef?: (value: unknown) => bigint,
): Encoded {
  if (isRecord(value)) {
    const entries = Object.entries(value);
    if (entries.length !== 1)
      throw new TypeError(
        `value object is not a typed wrapper; wrap literal objects with {"json":...}`,
      );
    const [kind, inner] = entries[0] ?? [];
    if (kind === "ref") {
      if (resolveRef === undefined)
        throw new TypeError(
          `reference ${String(inner)} cannot be resolved here; provide an entity id or name`,
        );
      const target = resolveRef(inner);
      return { tag: REF, stored: target, logical: target };
    }
    if (kind === "instant") {
      const instant = instantValue(inner);
      return { tag: INSTANT, stored: instant, logical: instant };
    }
    if (kind === "bytes") {
      const data = bytesValue(inner);
      return data.length > BLOB_THRESHOLD
        ? {
            tag: BYTES_REF,
            stored: indirectDigest(BYTES_REF, data),
            logical: data,
            blob: data,
          }
        : { tag: BYTES, stored: data, logical: data };
    }
    if (kind === "vector") {
      const { logical, packed } = vectorValue(inner);
      return {
        tag: VECTOR,
        stored: indirectDigest(VECTOR, packed),
        logical,
        blob: packed,
      };
    }
    if (kind === "json") {
      const canonical = canonicalValueJson(inner);
      checkSize(Buffer.from(canonical), inner);
      return { tag: JSON_TAG, stored: canonical, logical: canonical };
    }
    throw new TypeError(
      `unknown typed wrapper ${String(kind)}; use ref, instant, bytes, vector, or json (tmp is only valid as an id)`,
    );
  }
  if (value === null || value === undefined)
    throw new TypeError(
      `null is not a fact scalar; wrap it as {"json":null} when null is domain data`,
    );
  if (typeof value === "boolean")
    return { tag: BOOL, stored: value ? 1n : 0n, logical: value };
  if (value instanceof JsonFloat) {
    if (!Number.isFinite(value.value))
      throw new TypeError(
        `float ${String(value)} is not finite; NaN and infinity are not supported`,
      );
    return { tag: FLOAT, stored: value.value, logical: value.value };
  }
  if (typeof value === "bigint") {
    const integer = asInt64(value);
    return { tag: INT, stored: integer, logical: integer };
  }
  if (typeof value === "number") {
    if (!Number.isFinite(value))
      throw new TypeError(
        `float ${String(value)} is not finite; NaN and infinity are not supported`,
      );
    if (Number.isInteger(value)) {
      const integer = asInt64(value);
      return { tag: INT, stored: integer, logical: integer };
    }
    return { tag: FLOAT, stored: value, logical: value };
  }
  if (typeof value === "string") {
    const data = utf8(value);
    checkSize(data, value);
    return data.length > BLOB_THRESHOLD
      ? {
          tag: TEXT_REF,
          stored: indirectDigest(TEXT_REF, data),
          logical: value,
          blob: value,
        }
      : { tag: TEXT, stored: value, logical: value };
  }
  if (Buffer.isBuffer(value) || value instanceof Uint8Array) {
    const data = bytesValue(value);
    return data.length > BLOB_THRESHOLD
      ? {
          tag: BYTES_REF,
          stored: indirectDigest(BYTES_REF, data),
          logical: data,
          blob: data,
        }
      : { tag: BYTES, stored: data, logical: data };
  }
  throw new TypeError(
    `unsupported fact value ${String(value)}; use a scalar or a typed wrapper`,
  );
}

export function typeName(tag: number): string {
  const name = TAG_NAMES[tag];
  if (name === undefined)
    throw new TypeError(
      `unknown stored value tag ${tag}; run doctor to inspect file corruption`,
    );
  return name;
}

export function valueMatches(declared: string | null, value: Encoded): boolean {
  return declared === null || typeName(value.tag) === declared;
}

export function instantText(microseconds: bigint): string {
  const instant = instantValue(microseconds);
  let seconds = instant / 1_000_000n;
  let micros = instant % 1_000_000n;
  if (micros < 0) {
    seconds -= 1n;
    micros += 1_000_000n;
  }
  const iso = new Date(Number(seconds * 1000n)).toISOString();
  return `${iso.slice(0, 19)}.${micros.toString().padStart(6, "0")}Z`;
}

export function wireValue(
  tag: number,
  logical: unknown,
  nameForId: (id: bigint) => string | number | bigint,
): unknown {
  if (tag === REF) return { ref: nameForId(logical as bigint) };
  if (tag === INSTANT) return { instant: instantText(logical as bigint) };
  if (tag === BYTES || tag === BYTES_REF)
    return { bytes: (logical as Buffer).toString("base64") };
  if (tag === VECTOR) return { vector: logical };
  if (tag === JSON_TAG)
    return {
      json: materializeJson(parseJsonValue(logical as string, "stored JSON")),
    };
  if (tag === INT) return publicInteger(logical as bigint);
  return logical;
}

function materializeJson(value: unknown): unknown {
  if (value instanceof JsonFloat) return value.value;
  if (Array.isArray(value)) return value.map(materializeJson);
  if (value !== null && typeof value === "object")
    return Object.fromEntries(
      Object.entries(value).map(([key, item]) => [key, materializeJson(item)]),
    );
  return value;
}

export function publicInteger(value: bigint): number | bigint {
  return value >= BigInt(Number.MIN_SAFE_INTEGER) &&
    value <= BigInt(Number.MAX_SAFE_INTEGER)
    ? Number(value)
    : value;
}

export function isRecord(value: unknown): value is Record<string, unknown> {
  return (
    typeof value === "object" &&
    value !== null &&
    !Array.isArray(value) &&
    !ArrayBuffer.isView(value) &&
    !(value instanceof JsonFloat)
  );
}

export function equalStored(left: Stored, right: Stored): boolean {
  return Buffer.isBuffer(left) && Buffer.isBuffer(right)
    ? left.equals(right)
    : left === right;
}

export function storedKey(value: Stored): string {
  if (Buffer.isBuffer(value)) return `b:${value.toString("hex")}`;
  return `${typeof value}:${String(value)}`;
}
