export function normalizeRating(value) {
  const numeric = Number(value);
  if (!Number.isFinite(numeric)) {
    return 0;
  }
  const rounded = Math.round(numeric);
  return rounded >= 1 && rounded <= 5 ? rounded : 0;
}

export function parseStoredRatings(raw) {
  const ratings = new Map();
  if (!raw) {
    return ratings;
  }

  let payload;
  try {
    payload = JSON.parse(raw);
  } catch {
    return ratings;
  }

  if (!payload || typeof payload !== "object" || Array.isArray(payload)) {
    return ratings;
  }

  Object.entries(payload).forEach(([stationId, value]) => {
    const id = String(stationId || "").trim();
    const rating = normalizeRating(value);
    if (id && rating > 0) {
      ratings.set(id, rating);
    }
  });
  return ratings;
}

export function serializeStoredRatings(ratings) {
  const entries = Array.from(ratings instanceof Map ? ratings.entries() : [])
    .map(([stationId, value]) => [String(stationId || "").trim(), normalizeRating(value)])
    .filter(([stationId, rating]) => stationId && rating > 0)
    .sort(([left], [right]) => left.localeCompare(right));

  return JSON.stringify(Object.fromEntries(entries));
}

export function getUserRating(ratings, stationId) {
  const id = String(stationId || "").trim();
  if (!id || !(ratings instanceof Map)) {
    return 0;
  }
  return normalizeRating(ratings.get(id));
}

export function formatRatingValue(rating) {
  const normalized = normalizeRating(rating);
  if (!normalized) {
    return "";
  }
  return normalized.toFixed(1).replace(".", ",");
}
