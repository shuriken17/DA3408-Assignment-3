import glob, json, os, re, statistics as st
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import pandas as pd

os.makedirs("charts", exist_ok=True)
MODES = ["python", "pandas", "native"]
FW = ["spark", "ray"]
COL = {"spark": "#E25A1C", "ray": "#028CF0"}


def e2e(fw, m):
    return [json.load(open(f))["total_seconds"] for f in sorted(glob.glob("results/%s_%s_bench*_e2e.json" % (fw, m)))]


def mon(fw, m):
    cp, cm, mp = [], [], []
    for f in sorted(glob.glob("results/monitor/%s_%s_bench*.summary.txt" % (fw, m))):
        x = re.search(r"CPU peak (\d+)% mean (\d+)% \| system memory peak (\d+) MB", open(f).read())
        if x:
            cp.append(int(x[1])); cm.append(int(x[2])); mp.append(int(x[3]))
    return cp, cm, mp


data = {(fw, m): e2e(fw, m) for fw in FW for m in MODES}
lines = ["| Framework | UDF mode | Runs | Min (s) | Median (s) | Max (s) | CPU peak % | CPU mean % | System memory peak (MB) |",
         "|---|---|---|---|---|---|---|---|---|"]
for fw in FW:
    for m in MODES:
        t = data[(fw, m)]
        cp, cm, mp = mon(fw, m)
        lines.append("| %s | %s | %d | %.1f | %.1f | %.1f | %d | %d | %d |" % (fw, m, len(t), min(t), st.median(t), max(t), max(cp), round(sum(cm) / len(cm)), max(mp)))
open("results/benchmark_table.md", "w").write("\n".join(lines) + "\n")
print("\n".join(lines))
for fw in FW:
    nat = st.median(data[(fw, "native")])
    print("%s end-to-end overhead vs native (median): python %+.1fs, pandas %+.1fs" % (fw, st.median(data[(fw, "python")]) - nat, st.median(data[(fw, "pandas")]) - nat))

fig, ax = plt.subplots(figsize=(7.5, 4.2))
w = 0.36
for i, fw in enumerate(FW):
    xs = [j + (i - 0.5) * w for j in range(3)]
    med = [st.median(data[(fw, m)]) for m in MODES]
    err = [[med[j] - min(data[(fw, m)]) for j, m in enumerate(MODES)], [max(data[(fw, m)]) - med[j] for j, m in enumerate(MODES)]]
    ax.bar(xs, med, w, yerr=err, capsize=4, color=COL[fw], label=fw.capitalize())
    for j, m in enumerate(MODES):
        ax.scatter([xs[j]] * len(data[(fw, m)]), data[(fw, m)], color="black", s=9, zorder=3)
ax.set_xticks(range(3))
ax.set_xticklabels(["Python UDF", "pandas UDF", "native (no Python)"])
ax.set_ylabel("End-to-end time (s)")
ax.set_title("Ingestion-to-export time, 11.7M rows (bar = median, whisker = min/max, dots = runs)", fontsize=9)
ax.legend()
fig.tight_layout()
fig.savefig("charts/e2e_time.png", dpi=150)
plt.close(fig)
print("chart: charts/e2e_time.png")

try:
    sp = {s["stage"]: s["seconds"] for s in json.load(open("results/spark_python_udftest_inst.json"))["stages"]}
    ry = {s["stage"]: s["seconds"] for s in json.load(open("results/ray_python_full_inst.json"))["stages"]}
    groups = ["ingest + cleanse (+ dedup)", "zone join (Join A)", "Python UDF", "heavy join (Join B)", "export"]
    spv = [sp["ingest"] + sp["cleanse"], sp["join_zones"], sp["apply_udf"], sp["join_heavy"], 0]
    ryv = [ry["ingest_cleanse"] + ry["dedup"], ry["join_zones"], ry["apply_udf"], ry["join_heavy"], ry["export"]]
    print("stage seconds, Spark:", [round(v, 1) for v in spv], "Ray:", [round(v, 1) for v in ryv])
    fig, ax = plt.subplots(figsize=(8.5, 3.4))
    colors = ["#4C78A8", "#F58518", "#54A24B", "#E45756", "#B279A2"]
    for k, g in enumerate(groups):
        ax.barh(["Spark", "Ray"], [spv[k], ryv[k]], left=[sum(spv[:k]), sum(ryv[:k])], color=colors[k], label=g)
    ax.set_xlabel("seconds (one instrumented run, Python UDF)")
    ax.set_title("Stage breakdown (the Spark run had no export stage; Ray ingest = 8 passes over the files)", fontsize=8)
    ax.legend(fontsize=7, ncol=2, loc="lower right")
    fig.tight_layout()
    fig.savefig("charts/stage_breakdown.png", dpi=150)
    plt.close(fig)
    print("chart: charts/stage_breakdown.png")
except Exception as e:
    print("stage chart skipped:", e)

try:
    mic = {fw: json.load(open("results/udf_micro_%s.json" % fw)) for fw in FW}
    fig, axs = plt.subplots(1, 2, figsize=(9.5, 3.8))
    w = 0.36
    for i, fw in enumerate(FW):
        xs = [j + (i - 0.5) * w for j in range(3)]
        med = [st.median(mic[fw]["runs"][m]) for m in MODES]
        axs[0].bar(xs, med, w, color=COL[fw], label=fw.capitalize())
        for j, m in enumerate(MODES):
            axs[0].scatter([xs[j]] * len(mic[fw]["runs"][m]), mic[fw]["runs"][m], color="black", s=9, zorder=3)
        axs[1].bar([0 + (i - 0.5) * w, 1 + (i - 0.5) * w], [med[0] - med[2], med[1] - med[2]], w, color=COL[fw])
        print("UDF-only %s: python %.1fs, pandas %.1fs, native %.1fs | overhead vs native: python %+.1fs (%.0f%%), pandas %+.1fs (%.0f%%)" % (
            fw, med[0], med[1], med[2], med[0] - med[2], 100 * (med[0] - med[2]) / med[2], med[1] - med[2], 100 * (med[1] - med[2]) / med[2]))
    axs[0].set_xticks(range(3))
    axs[0].set_xticklabels(["Python", "pandas", "native"])
    axs[0].set_ylabel("seconds (UDF stage only, median of 3)")
    axs[0].set_title("UDF-only time", fontsize=9)
    axs[0].legend()
    axs[1].set_xticks(range(2))
    axs[1].set_xticklabels(["Python UDF", "pandas UDF"])
    axs[1].set_ylabel("seconds above native")
    axs[1].set_title("Overhead vs native", fontsize=9)
    fig.tight_layout()
    fig.savefig("charts/udf_micro.png", dpi=150)
    plt.close(fig)
    sums = set(mic[fw]["checksum"][m] for fw in FW for m in MODES)
    print("speed_centi sums identical across all 6 mode/framework combinations:", len(sums) == 1)
    print("chart: charts/udf_micro.png")
except Exception as e:
    print("UDF micro chart skipped:", e)

try:
    fig, axs = plt.subplots(2, 1, figsize=(8, 5), sharex=True)
    for fw in FW:
        t = data[(fw, "python")]
        k = min(range(len(t)), key=lambda i: abs(t[i] - st.median(t))) + 1
        d = pd.read_csv("results/monitor/%s_python_bench%d.csv" % (fw, k))
        axs[0].plot(d.t_sec, d.cpu_pct, color=COL[fw], lw=1, label="%s (run %d, %.0fs)" % (fw.capitalize(), k, t[k - 1]))
        axs[1].plot(d.t_sec, d.sys_mem_used_mb, color=COL[fw], lw=1)
    axs[0].set_ylabel("CPU % (all cores)")
    axs[1].set_ylabel("System memory used (MB)")
    axs[1].set_xlabel("seconds since start")
    axs[0].legend()
    fig.tight_layout()
    fig.savefig("charts/cpu_mem_over_time.png", dpi=150)
    plt.close(fig)
    print("chart: charts/cpu_mem_over_time.png")
except Exception as e:
    print("cpu/mem chart skipped:", e)
