import test from "node:test";
import assert from "node:assert/strict";

import {
  parseServerTiming,
  percentile,
  summarizeBenchmarkRuns,
} from "./live-loadtest.mjs";

test("parseServerTiming extracts durations and descriptions", () => {
  const parsed = parseServerTiming(
    'db-query;dur=12.3;desc="SQLite query", json-encode;dur=1.1;desc="JSON encode", app;dur=19.4',
  );

  assert.deepEqual(parsed, {
    "db-query": { dur: 12.3, desc: "SQLite query" },
    "json-encode": { dur: 1.1, desc: "JSON encode" },
    app: { dur: 19.4, desc: "" },
  });
});

test("percentile returns null for empty input and the expected rank otherwise", () => {
  assert.equal(percentile([], 0.95), null);
  assert.equal(percentile([10, 30, 20, 40], 0.5), 20);
  assert.equal(percentile([10, 30, 20, 40], 0.95), 40);
});

test("summarizeBenchmarkRuns ignores failed runs for latency aggregates", () => {
  const summary = summarizeBenchmarkRuns([
    {
      ok: true,
      totalMs: 100,
      requestEncodeMs: 1,
      headersMs: 70,
      bodyReadMs: 10,
      responseParseMs: 2,
      serverAppMs: 40,
      serverDbQueryMs: 20,
      serverDbDecodeMs: 3,
      serverPayloadMs: 4,
      serverJsonEncodeMs: 5,
      networkGapMs: 30,
      resourceTtfbMs: 65,
      resourceDownloadMs: 5,
    },
    {
      ok: true,
      totalMs: 200,
      requestEncodeMs: 2,
      headersMs: 150,
      bodyReadMs: 20,
      responseParseMs: 4,
      serverAppMs: 90,
      serverDbQueryMs: 60,
      serverDbDecodeMs: 5,
      serverPayloadMs: 6,
      serverJsonEncodeMs: 7,
      networkGapMs: 60,
      resourceTtfbMs: 140,
      resourceDownloadMs: 10,
    },
    {
      ok: false,
      totalMs: 999,
      requestEncodeMs: 0,
      headersMs: 0,
      bodyReadMs: 0,
      responseParseMs: 0,
      serverAppMs: 0,
      serverDbQueryMs: 0,
      serverDbDecodeMs: 0,
      serverPayloadMs: 0,
      serverJsonEncodeMs: 0,
      networkGapMs: 0,
      resourceTtfbMs: 0,
      resourceDownloadMs: 0,
    },
  ]);

  assert.equal(summary.totalCount, 3);
  assert.equal(summary.okCount, 2);
  assert.equal(summary.errorCount, 1);
  assert.equal(summary.metrics.totalMs.mean, 150);
  assert.equal(summary.metrics.totalMs.p50, 100);
  assert.equal(summary.metrics.totalMs.p95, 200);
  assert.equal(summary.metrics.serverDbQueryMs.mean, 40);
  assert.equal(summary.metrics.serverJsonEncodeMs.p95, 7);
});
