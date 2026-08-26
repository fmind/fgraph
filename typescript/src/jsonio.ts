import { TooLarge, TypeError } from "./errors.js";

export const MAX_JSON_DEPTH = 64;
export const MAX_JSON_DOCUMENT_DEPTH = 80;

/** JSON value with bigint added so signed int64 tokens remain lossless. */
export class JsonFloat {
  readonly value: number;

  constructor(value: number) {
    this.value = value;
  }

  valueOf(): number {
    return this.value;
  }

  toString(): string {
    return String(this.value);
  }
}

export type JsonValue =
  | null
  | boolean
  | number
  | bigint
  | JsonFloat
  | string
  | JsonValue[]
  | { [key: string]: JsonValue };

class Parser {
  readonly #source: string;
  readonly #context: string;
  readonly #maxDepth: number;
  #index = 0;

  constructor(source: string, context: string, maxDepth: number) {
    this.#source = source;
    this.#context = context;
    this.#maxDepth = maxDepth;
  }

  parse(): JsonValue {
    this.#space();
    const value = this.#value(0);
    this.#space();
    if (this.#index !== this.#source.length)
      this.#fail("unexpected trailing content");
    return value;
  }

  #value(depth: number): JsonValue {
    const char = this.#source[this.#index];
    if (char === '"') return this.#string();
    if (char === "{") {
      this.#checkDepth(depth);
      return this.#object(depth + 1);
    }
    if (char === "[") {
      this.#checkDepth(depth);
      return this.#array(depth + 1);
    }
    if (char === "t") return this.#literal("true", true);
    if (char === "f") return this.#literal("false", false);
    if (char === "n") return this.#literal("null", null);
    if (char === "-" || (char !== undefined && char >= "0" && char <= "9"))
      return this.#number();
    this.#fail("expected a JSON value");
  }

  #literal<T extends null | boolean>(text: string, value: T): T {
    if (!this.#source.startsWith(text, this.#index))
      this.#fail(`expected ${text}`);
    this.#index += text.length;
    return value;
  }

  #string(): string {
    const start = this.#index;
    this.#index++;
    while (this.#index < this.#source.length) {
      const char = this.#source[this.#index++];
      if (char === '"') {
        const token = this.#source.slice(start, this.#index);
        try {
          const result: unknown = JSON.parse(token);
          if (typeof result !== "string" || !result.isWellFormed()) {
            this.#fail("string contains an unpaired surrogate");
          }
          return result;
        } catch (error) {
          if (error instanceof TypeError) throw error;
          this.#fail("invalid JSON string");
        }
      }
      if (char === "\\") {
        const escape = this.#source[this.#index++];
        if (escape === "u") {
          const hex = this.#source.slice(this.#index, this.#index + 4);
          if (!/^[0-9a-fA-F]{4}$/u.test(hex))
            this.#fail("invalid Unicode escape");
          this.#index += 4;
        } else if (escape === undefined || !'"\\/bfnrt'.includes(escape)) {
          this.#fail("invalid string escape");
        }
      } else if (char !== undefined && char.charCodeAt(0) < 0x20) {
        this.#fail("unescaped control character in string");
      }
    }
    this.#fail("unterminated string");
  }

  #number(): number | bigint | JsonFloat {
    const rest = this.#source.slice(this.#index);
    const match = /^-?(?:0|[1-9][0-9]*)(?:\.[0-9]+)?(?:[eE][+-]?[0-9]+)?/u.exec(
      rest,
    );
    if (match === null) this.#fail("invalid JSON number");
    const token = match[0];
    this.#index += token.length;
    if (!token.includes(".") && !/[eE]/u.test(token)) {
      const value = BigInt(token);
      return value >= BigInt(Number.MIN_SAFE_INTEGER) &&
        value <= BigInt(Number.MAX_SAFE_INTEGER)
        ? Number(value)
        : value;
    }
    const value = Number(token);
    if (!Number.isFinite(value)) this.#fail("non-finite JSON number");
    return new JsonFloat(value);
  }

  #array(depth: number): JsonValue[] {
    this.#index++;
    this.#space();
    const result: JsonValue[] = [];
    if (this.#source[this.#index] === "]") {
      this.#index++;
      return result;
    }
    while (true) {
      result.push(this.#value(depth));
      this.#space();
      const separator = this.#source[this.#index++];
      if (separator === "]") return result;
      if (separator !== ",") this.#fail("expected ',' or ']'");
      this.#space();
    }
  }

  #object(depth: number): { [key: string]: JsonValue } {
    this.#index++;
    this.#space();
    const result: { [key: string]: JsonValue } = {};
    if (this.#source[this.#index] === "}") {
      this.#index++;
      return result;
    }
    while (true) {
      if (this.#source[this.#index] !== '"')
        this.#fail("expected an object key");
      const key = this.#string();
      if (Object.hasOwn(result, key)) {
        throw new TypeError(
          `duplicate JSON key ${JSON.stringify(key)}; keep one value so transaction intent is unambiguous`,
        );
      }
      this.#space();
      if (this.#source[this.#index++] !== ":")
        this.#fail("expected ':' after object key");
      this.#space();
      // Defining a data property keeps the valid JSON key "__proto__" from
      // invoking Object.prototype's legacy prototype setter.
      Object.defineProperty(result, key, {
        configurable: true,
        enumerable: true,
        value: this.#value(depth),
        writable: true,
      });
      this.#space();
      const separator = this.#source[this.#index++];
      if (separator === "}") return result;
      if (separator !== ",") this.#fail("expected ',' or '}'");
      this.#space();
    }
  }

  #space(): void {
    while (/\s/u.test(this.#source[this.#index] ?? "")) this.#index++;
  }

  #checkDepth(depth: number): void {
    if (depth >= this.#maxDepth)
      throw new TooLarge(
        `${this.#context} exceeds the maximum JSON nesting depth of ${this.#maxDepth}; flatten deeply nested arrays and objects`,
      );
  }

  #fail(message: string): never {
    throw new TypeError(
      `${this.#context} is not valid JSON (${message} at column ${this.#index + 1}); correct the JSON syntax`,
    );
  }
}

export function parseJson(source: string, context = "input"): JsonValue {
  return new Parser(source, context, MAX_JSON_DOCUMENT_DEPTH).parse();
}

export function parseJsonValue(
  source: string,
  context = "JSON value",
): JsonValue {
  return new Parser(source, context, MAX_JSON_DEPTH).parse();
}

function encodeString(value: string): string {
  if (!value.isWellFormed()) {
    throw new TypeError(
      `string ${JSON.stringify(value)} is not valid UTF-8; remove unpaired surrogate code points`,
    );
  }
  return JSON.stringify(value);
}

function numberText(value: number): string {
  if (!Number.isFinite(value))
    throw new TypeError(
      `number ${String(value)} is not finite; use a finite JSON number`,
    );
  if (Object.is(value, -0)) return "0";
  return String(value);
}

function canonicalNumber(value: number): string {
  if (!Number.isFinite(value))
    throw new TypeError(
      `number ${String(value)} is not finite; use a finite JSON number`,
    );
  if (value === 0) return "0";
  const integer = Number.isInteger(value);
  if (integer) {
    const exact = BigInt(value);
    if (exact >= -(2n ** 63n) && exact <= 2n ** 63n - 1n)
      return exact.toString();
  }
  let rendered = String(value).toLowerCase();
  if (integer && !rendered.includes("e"))
    rendered = value.toExponential().toLowerCase();
  if (!rendered.includes("e"))
    return rendered.endsWith(".0") ? rendered.slice(0, -2) : rendered;
  let [mantissa = "0", exponentText = "0"] = rendered.split("e");
  const exponent = Number(exponentText);
  // ECMAScript already renders finite non-integers in decimal notation throughout
  // [1e-6, 1e21), which is exactly the canonical JSON decimal window.
  mantissa = mantissa.endsWith(".0") ? mantissa.slice(0, -2) : mantissa;
  return `${mantissa}e${exponent >= 0 ? "+" : "-"}${Math.abs(exponent)}`;
}

/** JSON.stringify equivalent that serializes bigint as signed integer tokens. */
export function stringifyJson(value: unknown, pretty = false): string {
  const compact = stringifyPart(value, false, 0, MAX_JSON_DOCUMENT_DEPTH, true);
  return pretty ? prettyJson(compact) : compact;
}

function prettyJson(compact: string): string {
  let output = "";
  let depth = 0;
  let inString = false;
  let escaped = false;
  const indent = (): string => "  ".repeat(depth);
  for (let index = 0; index < compact.length; index++) {
    const character = compact[index] as string;
    if (inString) {
      output += character;
      if (escaped) escaped = false;
      else if (character === "\\") escaped = true;
      else if (character === '"') inString = false;
      continue;
    }
    if (character === '"') {
      inString = true;
      output += character;
    } else if (character === "{" || character === "[") {
      output += character;
      depth++;
      const closing = character === "{" ? "}" : "]";
      if (compact[index + 1] !== closing) output += `\n${indent()}`;
    } else if (character === "}" || character === "]") {
      depth--;
      const opening = character === "}" ? "{" : "[";
      if (compact[index - 1] !== opening) output += `\n${indent()}`;
      output += character;
    } else if (character === ",") output += `,\n${indent()}`;
    else if (character === ":") output += ": ";
    else output += character;
  }
  return output;
}

export function canonicalJson(value: unknown): string {
  return stringifyPart(value, true, 0, MAX_JSON_DOCUMENT_DEPTH, true);
}

export function canonicalValueJson(value: unknown): string {
  return stringifyPart(value, true, 0, MAX_JSON_DEPTH, false);
}

function denseArrayValues(value: unknown[]): unknown[] {
  const keys = Reflect.ownKeys(value).filter((key) => key !== "length");
  if (keys.length !== value.length)
    throw new TypeError(
      "JSON arrays must be dense and contain no named or symbol properties",
    );
  const items: unknown[] = [];
  for (let index = 0; index < value.length; index++) {
    const descriptor = Object.getOwnPropertyDescriptor(value, String(index));
    if (descriptor === undefined || !("value" in descriptor))
      throw new TypeError(
        "JSON arrays must contain only concrete data values, not holes or accessors",
      );
    items.push(descriptor.value);
  }
  return items;
}

function plainObjectEntries(
  value: object,
  allowByteViews: boolean,
): Array<[string, unknown]> {
  const prototype = Object.getPrototypeOf(value);
  const byteView = Buffer.isBuffer(value) || value instanceof Uint8Array;
  if (
    prototype !== Object.prototype &&
    prototype !== null &&
    !(allowByteViews && byteView)
  )
    throw new TypeError(
      "JSON objects must be plain objects; convert class instances and collection types to JSON data",
    );
  const entries: Array<[string, unknown]> = [];
  for (const key of Reflect.ownKeys(value)) {
    if (typeof key !== "string")
      throw new TypeError(
        "JSON objects cannot contain symbol properties; use string keys",
      );
    const descriptor = Object.getOwnPropertyDescriptor(value, key);
    if (
      descriptor === undefined ||
      !descriptor.enumerable ||
      !("value" in descriptor)
    )
      throw new TypeError(
        "JSON objects must contain only enumerable data properties, not hidden properties or accessors",
      );
    entries.push([key, descriptor.value]);
  }
  return entries;
}

function stringifyPart(
  value: unknown,
  canonical: boolean,
  depth: number,
  maxDepth: number,
  allowByteViews: boolean,
): string {
  if (value === null) return "null";
  if (value === true) return "true";
  if (value === false) return "false";
  if (typeof value === "bigint") {
    if (value < -(2n ** 63n) || value > 2n ** 63n - 1n)
      throw new TypeError("JSON integer exceeds signed 64-bit range");
    return value.toString();
  }
  if (value instanceof JsonFloat)
    return canonical ? canonicalNumber(value.value) : numberText(value.value);
  if (typeof value === "number")
    return canonical ? canonicalNumber(value) : numberText(value);
  if (typeof value === "string") return encodeString(value);
  if (Array.isArray(value)) {
    if (depth >= maxDepth)
      throw new TooLarge(
        `JSON value exceeds the maximum nesting depth of ${maxDepth}; flatten deeply nested arrays and objects`,
      );
    return `[${denseArrayValues(value)
      .map((item) =>
        stringifyPart(item, canonical, depth + 1, maxDepth, allowByteViews),
      )
      .join(",")}]`;
  }
  if (typeof value === "object") {
    if (depth >= maxDepth)
      throw new TooLarge(
        `JSON value exceeds the maximum nesting depth of ${maxDepth}; flatten deeply nested arrays and objects`,
      );
    const entries = plainObjectEntries(value, allowByteViews);
    if (canonical)
      entries.sort(([left], [right]) => compareUnicode(left, right));
    return `{${entries
      .map(
        ([key, item]) =>
          `${encodeString(key)}:${stringifyPart(
            item,
            canonical,
            depth + 1,
            maxDepth,
            allowByteViews,
          )}`,
      )
      .join(",")}}`;
  }
  throw new TypeError(
    `unsupported JSON value ${String(value)}; use finite JSON scalars, arrays, and objects`,
  );
}

export function compareUnicode(left: string, right: string): number {
  const a = [...left];
  const b = [...right];
  for (let index = 0; index < Math.min(a.length, b.length); index++) {
    const leftCode = a[index]!.codePointAt(0) as number;
    const rightCode = b[index]!.codePointAt(0) as number;
    if (leftCode !== rightCode) return leftCode - rightCode;
  }
  return a.length - b.length;
}
