import sys
import numpy as np, pandas as pd, pyarrow as pa, pyarrow.compute as pc, pyarrow.dataset as ds

NULL_MARK = np.uint64(0x9E3779B97F4A7C15)

def norm_type(t):
    return "timestamp[us]" if pa.types.is_timestamp(t) else str(t)

def col_hash(col):
    """Type-independent per-value hash: ints/dates/timestamps -> int64, floats -> float64, nulls -> fixed marker."""
    mask = col.is_null().to_numpy(zero_copy_only=False)
    t = col.type
    if pa.types.is_timestamp(t):
        v = pc.cast(pc.cast(col, pa.timestamp("us")), pa.int64())
    elif pa.types.is_date(t):
        v = pc.cast(pc.cast(col, pa.int32()), pa.int64())
    elif pa.types.is_integer(t):
        v = pc.cast(col, pa.int64())
    elif pa.types.is_floating(t):
        v = pc.cast(col, pa.float64())
    else:
        v = pc.cast(col, pa.string())
    if pa.types.is_string(v.type):
        arr = pc.fill_null(v, "").to_numpy(zero_copy_only=False)
    elif pa.types.is_floating(v.type):
        arr = pc.fill_null(v, 0.0).to_numpy(zero_copy_only=False)
    else:
        arr = pc.fill_null(v, 0).to_numpy(zero_copy_only=False)
    h = pd.util.hash_array(arr)
    return np.where(mask, NULL_MARK, h)

def summarize(path):
    d = ds.dataset(path, format="parquet")
    raw = [(f.name, str(f.type)) for f in d.schema]
    norm = [(f.name, norm_type(f.type)) for f in d.schema]
    colsum = {n: 0 for n in d.schema.names}
    hashes, rows = [], 0
    for b in d.to_batches(batch_size=250_000):
        t = pa.Table.from_batches([b])
        rows += t.num_rows
        row = np.zeros(t.num_rows, dtype=np.uint64)
        for n in t.column_names:
            h = col_hash(t[n].combine_chunks() if hasattr(t[n], "combine_chunks") else t[n])
            colsum[n] = (colsum[n] + int(h.sum(dtype=np.uint64))) % (1 << 64)
            row = row * np.uint64(1000003) + h
        hashes.append(row)
        del t
    return {"raw": raw, "norm": norm, "rows": rows, "colsum": colsum, "hashes": np.sort(np.concatenate(hashes))}

a_path, b_path = sys.argv[1], sys.argv[2]
a, b = summarize(a_path), summarize(b_path)
ok = True
print("==== parity:", a_path, "vs", b_path)
diff = [(x, y) for x, y in zip(a["raw"], b["raw"]) if x != y]
print("raw schema differences (name, type) first vs second:", diff if diff else "none")
print("schema equal after timestamp normalisation:", a["norm"] == b["norm"]); ok &= a["norm"] == b["norm"]
print("rows:", a["rows"], b["rows"], "| equal:", a["rows"] == b["rows"]); ok &= a["rows"] == b["rows"]
bad = [n for n in a["colsum"] if a["colsum"][n] != b["colsum"].get(n)]
print("columns with different checksum:", bad if bad else "none"); ok &= not bad
same = a["hashes"].shape == b["hashes"].shape and bool((a["hashes"] == b["hashes"]).all())
print("row-set checksum equal:", same)
if not same:
    print("rows only in first:", len(np.setdiff1d(a["hashes"], b["hashes"])), "| only in second:", len(np.setdiff1d(b["hashes"], a["hashes"])))
ok &= same
print("PARITY", "PASS" if ok else "FAIL")
sys.exit(0 if ok else 1)
