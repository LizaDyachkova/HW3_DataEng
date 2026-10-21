#!/usr/bin/env python3
"""Профилирование: время и пиковая память команды, несколько запусков подряд.

Память — пик суммы RSS (резидентной памяти) всех процессов запущенной команды,
включая дочерние (процессы-мапперы). Замеряется каждые 50 мс.

    python profile_run.py --label mr-optimized --runs 3 -- python mapreduce_skills.py --mode optimized
    python profile_run.py --label duckdb       --runs 3 -- python duckdb_skills.py

Результаты дописываются в profiling/results.csv, лог каждого запуска — в profiling/logs/.
"""
import argparse
import csv
import os
import statistics
import subprocess
import sys
import threading
import time

import psutil


def sample_peak(proc, stop, out):
    peak_sum = peak_one = 0
    while not stop.is_set():
        total = 0
        try:
            procs = [proc] + proc.children(recursive=True)
        except psutil.Error:
            procs = []
        for p in procs:
            try:
                rss = p.memory_info().rss
            except psutil.Error:
                continue
            total += rss
            peak_one = max(peak_one, rss)
        peak_sum = max(peak_sum, total)
        time.sleep(0.05)
    out["peak_sum"], out["peak_one"] = peak_sum, peak_one


def main():
    argv = sys.argv[1:]
    if "--" not in argv:
        sys.exit("Использование: python profile_run.py --label имя --runs 3 -- команда ...")
    split = argv.index("--")
    ap = argparse.ArgumentParser()
    ap.add_argument("--label", required=True)
    ap.add_argument("--runs", type=int, default=3)
    args = ap.parse_args(argv[:split])
    cmd = argv[split + 1:]

    os.makedirs("profiling/logs", exist_ok=True)
    results = []
    for i in range(1, args.runs + 1):
        log_path = "profiling/logs/%s_run%d.log" % (args.label, i)
        t0 = time.time()
        with open(log_path, "w", encoding="utf-8") as log:
            proc = subprocess.Popen(cmd, stdout=log, stderr=subprocess.STDOUT)
            ps = psutil.Process(proc.pid)
            stop, mem = threading.Event(), {}
            th = threading.Thread(target=sample_peak, args=(ps, stop, mem))
            th.start()
            code = proc.wait()
            stop.set()
            th.join()
        sec = time.time() - t0
        row = {"label": args.label, "run": i, "sec": round(sec, 2),
               "peak_rss_total_mb": round(mem["peak_sum"] / 1048576, 1),
               "peak_rss_one_process_mb": round(mem["peak_one"] / 1048576, 1), "exit_code": code}
        results.append(row)
        print("запуск %d: %.1f с, пик памяти (все процессы) %.0f МБ, самый тяжёлый процесс %.0f МБ, код %d"
              % (i, sec, row["peak_rss_total_mb"], row["peak_rss_one_process_mb"], code), flush=True)

    new_file = not os.path.exists("profiling/results.csv")
    with open("profiling/results.csv", "a", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=list(results[0].keys()))
        if new_file:
            w.writeheader()
        w.writerows(results)

    secs = [r["sec"] for r in results]
    mems = [r["peak_rss_total_mb"] for r in results]
    print("-" * 70)
    print("%s: время  min %.1f / среднее %.1f / max %.1f с;  пик памяти  min %.0f / среднее %.0f / max %.0f МБ"
          % (args.label, min(secs), statistics.mean(secs), max(secs), min(mems), statistics.mean(mems), max(mems)))


if __name__ == "__main__":
    main()
