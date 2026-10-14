const encoder = new TextEncoder();
const tokens = (text) => text.match(/[\p{Alphabetic}\p{Number}]+/gu)?.map((s) => s.toLowerCase()) || [];

export function discover(catalog, query, requested) {
  if (encoder.encode(query).length > 4096) throw new RangeError("search query exceeds 4096 bytes");
  const queryTokens = tokens(query);
  if (queryTokens.length > 32) throw new RangeError("search query exceeds 32 tokens");
  const terms = [...new Set(queryTokens)].sort();
  const limit = requested === undefined ? 10 : Number.isFinite(requested) && requested >= 1 ? Math.min(50, Math.floor(requested)) : 1;
  const documents = catalog.map((entry) => {
    const frequencies = new Map();
    const words = tokens(`${entry.namespace || ""} ${entry.description}`);
    for (const word of words) frequencies.set(word, (frequencies.get(word) || 0) + 1);
    return { entry, frequencies, length: words.length, names: tokens(entry.name) };
  });
  const average = documents.reduce((sum, d) => sum + d.length, 0) / Math.max(1, documents.length);
  const idfs = terms.map((term) => {
    const df = documents.filter((d) => d.frequencies.has(term) || d.names.includes(term)).length;
    return Math.log((documents.length - df + 0.5) / (df + 0.5) + 1);
  });
  return documents.map((d) => {
    let score = 0;
    terms.forEach((term, i) => {
      if (d.names.includes(term)) score += idfs[i] * 2.2;
      const tf = d.frequencies.get(term);
      if (tf) score += idfs[i] * (tf * 2.2) / (tf + 1.2 * (0.25 + 0.75 * d.length / Math.max(1, average)));
    });
    return { score, entry: d.entry };
  }).filter((d) => d.score > 0).sort((a, b) => b.score - a.score || compareUtf8(a.entry.name, b.entry.name))
    .slice(0, limit).map((d) => describe(d.entry, false));
}

function compareUtf8(a, b) {
  const left = encoder.encode(a), right = encoder.encode(b);
  for (let i = 0; i < Math.min(left.length, right.length); i++) if (left[i] !== right[i]) return left[i] - right[i];
  return left.length - right.length;
}

export function describe(entry, full = true) {
  const result = { name: entry.name, description: entry.description };
  if (entry.namespace !== undefined) result.namespace = entry.namespace;
  if (full) {
    result.inputSchema = entry.input_schema;
    if (entry.output_schema !== undefined) result.outputSchema = entry.output_schema;
  }
  return result;
}
