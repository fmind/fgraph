import { describe, expect, it } from "vitest";

import * as api from "../src/index.js";
import { FGraphError, TooLarge, TypeError, errorName } from "../src/errors.js";
import {
  JsonFloat,
  canonicalJson,
  parseJson,
  stringifyJson,
} from "../src/jsonio.js";
import {
  BOOL,
  BYTES,
  BYTES_REF,
  FLOAT,
  INSTANT,
  INSTANT_MAX,
  INSTANT_MIN,
  INT,
  INT64_MAX,
  INT64_MIN,
  JSON_TAG,
  REF,
  TEXT,
  TEXT_REF,
  VECTOR,
  encode,
  equalStored,
  instantText,
  instantValue,
  isRecord,
  publicInteger,
  storedKey,
  typeName,
  valueMatches,
  wireValue,
} from "../src/values.js";

describe("public package and errors", () => {
  it("exports the version and reports typed versus native errors", () => {
    expect(api.version).toBe("1.1.0");
    expect(api.canonicalValueJson({ b: 2, a: 1 })).toBe('{"a":1,"b":2}');
    expect(errorName(new FGraphError("typed"))).toBe("FGraphError");
    expect(errorName(new Error("native"))).toBe("Error");
  });
});

describe("strict lossless JSON", () => {
  it("parses every JSON scalar/container without losing integer or float intent", () => {
    const float = parseJson("1.0");
    expect(float).toBeInstanceOf(JsonFloat);
    expect(Number(float)).toBe(1);
    expect(String(float)).toBe("1");
    expect(
      parseJson(
        ' { "empty": [], "object": {}, "truth": true, "falsehood": false, "nil": null, "escaped": "a\\nb", "large": 9007199254740993 } ',
      ),
    ).toEqual({
      empty: [],
      object: {},
      truth: true,
      falsehood: false,
      nil: null,
      escaped: "a\nb",
      large: 9_007_199_254_740_993n,
    });
    expect(parseJson("-12")).toBe(-12);
    expect(parseJson("2e2")).toEqual(new JsonFloat(200));
  });

  it.each([
    "",
    "truth",
    "@",
    "01",
    "-",
    "1e999",
    '"\\uD800"',
    '"\\uZZZZ"',
    '"\\x"',
    '"line\nbreak"',
    '"unterminated',
    "[1 2]",
    "[1,]",
    "{bad:1}",
    '{"a" 1}',
    '{"a":1 "b":2}',
    '{"a":1,}',
    '{"a":1,"a":2}',
  ])("rejects malformed input %j", (source) => {
    expect(() => parseJson(source, "test document")).toThrowError(TypeError);
  });

  it("renders presentation and canonical JSON across numeric branches", () => {
    expect(
      stringifyJson(
        { integer: 9_007_199_254_740_993n, float: new JsonFloat(1.5) },
        true,
      ),
    ).toContain("9007199254740993");
    expect(
      stringifyJson(
        {
          marker: "__fgraph_bigint_725e2673__9007199254740993",
          nested: [9_007_199_254_740_993n, { empty: {} }],
        },
        true,
      ),
    ).toBe(
      '{\n  "marker": "__fgraph_bigint_725e2673__9007199254740993",\n  "nested": [\n    9007199254740993,\n    {\n      "empty": {}\n    }\n  ]\n}',
    );
    expect(() => stringifyJson(undefined, true)).toThrowError(TypeError);
    expect(stringifyJson(new JsonFloat(-0))).toBe("0");
    expect(canonicalJson(null)).toBe("null");
    expect(canonicalJson(true)).toBe("true");
    expect(canonicalJson(false)).toBe("false");
    expect(canonicalJson(-0)).toBe("0");
    expect(canonicalJson(42)).toBe("42");
    expect(canonicalJson(1e20)).toBe("1e+20");
    expect(canonicalJson(new JsonFloat(1e-7))).toBe("1e-7");
    expect(canonicalJson(new JsonFloat(-1.2e-6))).toBe("-0.0000012");
    expect(canonicalJson(new JsonFloat(1.2e6))).toBe("1200000");
    expect(canonicalJson(new JsonFloat(1.23e4))).toBe("12300");
    expect(canonicalJson(new JsonFloat(1e21))).toBe("1e+21");
    expect(canonicalJson({ "😀": 1, aa: 2, a: 3, "\u2028": 4 })).toBe(
      '{"a":3,"aa":2," ":4,"😀":1}',
    );
    expect(stringifyJson({ b: 1, a: ["x"] })).toBe('{"b":1,"a":["x"]}');
    expect(() => canonicalJson(Number.NaN)).toThrowError(TypeError);
    expect(() => stringifyJson(Number.POSITIVE_INFINITY)).toThrowError(
      TypeError,
    );
    expect(() => canonicalJson("\uD800")).toThrowError(TypeError);
    expect(() => canonicalJson(Symbol("bad"))).toThrowError(TypeError);
  });
});

describe("fact value encoding", () => {
  it("parses and formats strict instants including offsets and negative fractions", () => {
    expect(instantValue(0)).toBe(0n);
    expect(instantValue(INSTANT_MIN)).toBe(INSTANT_MIN);
    expect(instantValue(INSTANT_MAX)).toBe(INSTANT_MAX);
    expect(instantValue("2026-08-24T10:00:00.123456789+02:30")).toBe(
      1_787_556_600_123_456n,
    );
    expect(instantText(-1n)).toBe("1969-12-31T23:59:59.999999Z");
    expect(() => instantValue(INT64_MIN)).toThrowError(TypeError);
    expect(() => instantValue(INT64_MAX)).toThrowError(TypeError);
    expect(() => instantValue(null)).toThrowError(TypeError);
    expect(() => instantValue("2026-08-24 10:00:00Z")).toThrowError(TypeError);
    expect(() => instantValue("2026-02-30T10:00:00Z")).toThrowError(TypeError);
    expect(() => instantValue("2026-08-24T10:00:00+24:00")).toThrowError(
      TypeError,
    );
    expect(() => instantValue("0001-01-01T00:00:00+23:59")).toThrowError(
      TypeError,
    );
  });

  it("encodes every scalar and typed wrapper with deterministic indirection", () => {
    expect(encode(true)).toMatchObject({
      tag: BOOL,
      stored: 1n,
      logical: true,
    });
    expect(encode(false)).toMatchObject({
      tag: BOOL,
      stored: 0n,
      logical: false,
    });
    expect(encode(INT64_MIN)).toMatchObject({ tag: INT, stored: INT64_MIN });
    expect(encode(12)).toMatchObject({ tag: INT, stored: 12n });
    expect(encode(1.25)).toMatchObject({ tag: FLOAT, stored: 1.25 });
    expect(encode(new JsonFloat(2))).toMatchObject({ tag: FLOAT, stored: 2 });
    expect(encode("short")).toMatchObject({ tag: TEXT, stored: "short" });
    expect(encode("x".repeat(257))).toMatchObject({
      tag: TEXT_REF,
      logical: "x".repeat(257),
      blob: "x".repeat(257),
    });
    expect(encode(Buffer.from("hi"))).toMatchObject({
      tag: BYTES,
      logical: Buffer.from("hi"),
    });
    expect(encode(new Uint8Array(257))).toMatchObject({ tag: BYTES_REF });
    expect(encode({ ref: "target" }, () => 77n)).toEqual({
      tag: REF,
      stored: 77n,
      logical: 77n,
    });
    expect(encode({ instant: "1970-01-01T00:00:00Z" })).toEqual({
      tag: INSTANT,
      stored: 0n,
      logical: 0n,
    });
    expect(encode({ bytes: "aGk=" })).toMatchObject({
      tag: BYTES,
      logical: Buffer.from("hi"),
    });
    expect(encode({ bytes: Buffer.alloc(257) })).toMatchObject({
      tag: BYTES_REF,
    });
    expect(encode({ vector: [new JsonFloat(0.1), 2n] })).toMatchObject({
      tag: VECTOR,
      logical: expect.any(Array),
      blob: expect.any(Buffer),
    });
    expect(encode({ json: { z: 1, a: null } })).toEqual({
      tag: JSON_TAG,
      stored: '{"a":null,"z":1}',
      logical: '{"a":null,"z":1}',
    });
  });

  it("rejects ambiguous, invalid, non-finite, and oversized values", () => {
    const nonFiniteJsonFloat = Object.assign(
      Object.create(JsonFloat.prototype) as JsonFloat,
      { value: Number.NaN },
    );
    expect(() => encode({})).toThrowError(TypeError);
    expect(() => encode({ a: 1, b: 2 })).toThrowError(TypeError);
    expect(() => encode({ ref: "missing" })).toThrowError(TypeError);
    expect(() => encode({ unknown: 1 })).toThrowError(TypeError);
    expect(() => encode(null)).toThrowError(TypeError);
    expect(() => encode(undefined)).toThrowError(TypeError);
    expect(() => encode(INT64_MAX + 1n)).toThrowError(TypeError);
    expect(() => encode(Number.MAX_SAFE_INTEGER + 1)).toThrowError(TypeError);
    expect(() => encode(Number.NaN)).toThrowError(TypeError);
    expect(() => encode(Number.POSITIVE_INFINITY)).toThrowError(TypeError);
    expect(() => encode(nonFiniteJsonFloat)).toThrowError(TypeError);
    expect(() => encode("\uD800")).toThrowError(TypeError);
    expect(() => encode("x".repeat(1_048_577))).toThrowError(TooLarge);
    expect(() => encode({ bytes: "not padded" })).toThrowError(TypeError);
    expect(() => encode({ bytes: 1 })).toThrowError(TypeError);
    expect(() => encode({ bytes: Buffer.alloc(1_048_577) })).toThrowError(
      TooLarge,
    );
    expect(() => encode({ vector: [] })).toThrowError(TypeError);
    expect(() => encode({ vector: "bad" })).toThrowError(TypeError);
    expect(() => encode({ vector: ["bad"] })).toThrowError(TypeError);
    expect(() => encode({ vector: [Number.POSITIVE_INFINITY] })).toThrowError(
      TypeError,
    );
    expect(() => encode({ vector: [Number.MAX_VALUE] })).toThrowError(
      TypeError,
    );
    expect(() =>
      encode({ vector: Array.from({ length: 262_145 }, () => 0) }),
    ).toThrowError(TooLarge);
    expect(() => encode({ json: "x".repeat(1_048_577) })).toThrowError(
      TooLarge,
    );
    expect(() => encode(Symbol("unsupported"))).toThrowError(TypeError);
  });

  it("renders and compares every physical tag", () => {
    const name = (id: bigint): string => `entity/${id}`;
    expect(Array.from({ length: 11 }, (_, tag) => typeName(tag))).toEqual([
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
    ]);
    expect(() => typeName(11)).toThrowError(TypeError);
    expect(valueMatches(null, encode(1))).toBe(true);
    expect(valueMatches("int", encode(1))).toBe(true);
    expect(valueMatches("text", encode(1))).toBe(false);
    expect(wireValue(REF, 4n, name)).toEqual({ ref: "entity/4" });
    expect(wireValue(INSTANT, 0n, name)).toEqual({
      instant: "1970-01-01T00:00:00.000000Z",
    });
    expect(wireValue(BYTES, Buffer.from("x"), name)).toEqual({ bytes: "eA==" });
    expect(wireValue(BYTES_REF, Buffer.from("y"), name)).toEqual({
      bytes: "eQ==",
    });
    expect(wireValue(VECTOR, [1], name)).toEqual({ vector: [1] });
    expect(wireValue(JSON_TAG, '{"x":1}', name)).toEqual({ json: { x: 1 } });
    expect(wireValue(INT, 9_007_199_254_740_993n, name)).toBe(
      9_007_199_254_740_993n,
    );
    expect(wireValue(TEXT, "text", name)).toBe("text");
    expect(publicInteger(1n)).toBe(1);
    expect(publicInteger(9_007_199_254_740_993n)).toBe(9_007_199_254_740_993n);
    expect(isRecord({})).toBe(true);
    expect(
      [null, [], Buffer.alloc(0), new JsonFloat(1), "object"].every(
        (value) => !isRecord(value),
      ),
    ).toBe(true);
    expect(equalStored(Buffer.from("a"), Buffer.from("a"))).toBe(true);
    expect(equalStored(Buffer.from("a"), Buffer.from("b"))).toBe(false);
    expect(equalStored("a", "a")).toBe(true);
    expect(storedKey(Buffer.from("a"))).toBe("b:61");
    expect(storedKey(1n)).toBe("bigint:1");
  });
});
