import test from "node:test";
import assert from "node:assert/strict";

import {
  formatRatingValue,
  getUserRating,
  normalizeRating,
  parseStoredRatings,
  serializeStoredRatings,
} from "./rating.mjs";

test("normalizes ratings to the supported 1-5 range", () => {
  assert.equal(normalizeRating(1), 1);
  assert.equal(normalizeRating("5"), 5);
  assert.equal(normalizeRating(3.4), 3);
  assert.equal(normalizeRating(0), 0);
  assert.equal(normalizeRating(6), 0);
  assert.equal(normalizeRating("nope"), 0);
});

test("parses and serializes stored station ratings", () => {
  const ratings = parseStoredRatings(JSON.stringify({
    "station-b": 5,
    "station-a": "4",
    "station-invalid": 9,
  }));

  assert.equal(getUserRating(ratings, "station-a"), 4);
  assert.equal(getUserRating(ratings, "station-b"), 5);
  assert.equal(getUserRating(ratings, "station-invalid"), 0);
  assert.equal(
    serializeStoredRatings(ratings),
    JSON.stringify({ "station-a": 4, "station-b": 5 }),
  );
});

test("formats ratings for German UI labels", () => {
  assert.equal(formatRatingValue(4), "4,0");
  assert.equal(formatRatingValue(0), "");
});
