import time
import statistics
import pandas as pd
import gc


def time_method(func, n_runs=5, warmup=1):
    for _ in range(warmup):
        func()

    times = []
    for _ in range(n_runs):
        gc.collect()
        start = time.perf_counter()
        func()
        elapsed = time.perf_counter() - start
        times.append(elapsed)

    return {
        "min": min(times),
        "max": max(times),
        "mean": statistics.mean(times),
        "median": statistics.median(times),
        "stdev": statistics.stdev(times) if len(times) > 1 else 0.0,
        "runs": n_runs,
        "raw": times,
    }


def run_benchmark_suite(methods, row_counts, n_runs=5, warmup=1, setup_fn=None, teardown_fn=None):
    from test_data import make_test_df

    rows = []
    total = len(methods) * len(row_counts)
    done = 0

    for n in row_counts:
        df = make_test_df(n)

        for name, method_fn in methods.items():
            done += 1
            print(f"  [{done}/{total}] {name} @ {n:,} rows ... ", end="", flush=True)

            def run_once():
                if setup_fn:
                    setup_fn(n)
                method_fn(df)
                if teardown_fn:
                    teardown_fn()

            try:
                result = time_method(run_once, n_runs=n_runs, warmup=warmup)
                rows.append({
                    "method": name,
                    "rows": n,
                    "min_sec": result["min"],
                    "max_sec": result["max"],
                    "mean_sec": result["mean"],
                    "median_sec": result["median"],
                    "stdev_sec": result["stdev"],
                    "rows_per_sec": n / result["median"] if result["median"] > 0 else 0,
                })
                print(f"{result['median']:.4f}s (median)")
            except Exception as e:
                print(f"FAILED: {e}")
                rows.append({
                    "method": name,
                    "rows": n,
                    "min_sec": None,
                    "max_sec": None,
                    "mean_sec": None,
                    "median_sec": None,
                    "stdev_sec": None,
                    "rows_per_sec": None,
                })

    return pd.DataFrame(rows)


def save_results(df, path="results/benchmark_results.csv"):
    df.to_csv(path, index=False)
    print(f"results saved to {path}")
