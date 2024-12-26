import json
import datetime as dt
import pyarrow as pa
import itertools as it
import pandas as pd
import os
import argparse
from pathlib import Path

def run(input_path: str | Path, output_path: str | Path):
    if not isinstance(input_path, Path):
        input_path = Path(input_path)
    if not isinstance(output_path, Path):
        output_path = Path(output_path)
    data = json.load(open(input_path / "benchmarks.json"))
    benchmark_to_param_names = {k: v["param_names"] for k, v in data.items() if k != "version"}

    results = json.load(open(input_path / "results.json"))
    commit_hash = results["commit_hash"]
    columns = results["result_columns"]
    buf = {"name": [], "params": [], "result": []}
    for name, benchmark in results['results'].items():
        data = dict(zip(columns, benchmark))
        result = data["result"]
        param_names = benchmark_to_param_names[name]
        params = [", ".join(f"{k}=v" for k, v in zip(param_names, e)) for e in it.product(*data["params"])]
        buf["name"].extend([name] * len(result))
        buf["params"].extend(params)
        buf["result"].extend(result)
    buf["name"] = pd.array(buf["name"], dtype="string[pyarrow]")
    buf["params"] = pd.array(buf["params"], dtype="string[pyarrow]")
    buf["result"] = pd.array(buf["result"], dtype="float64[pyarrow]")
    df = pd.DataFrame(buf)
    df["date"] = pd.array([dt.datetime.today()] * len(df), dtype=pd.ArrowDtype(pa.timestamp("us")))
    df["sha"] = pd.array([commit_hash] * len(df), dtype="string[pyarrow]")
    df = df[["date", "sha", "name", "params", "result"]]

    parquet_path = output_path / "results.parquet"
    if os.path.exists(parquet_path):
        existing = pd.read_parquet(parquet_path)
        final = pd.concat([existing, df])
    else:
        final = df
    final.to_parquet(parquet_path)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--input-path")
    parser.add_argument("--output-path")
    args = parser.parse_args()
    run(input_path=args.input_path, output_path=args.output_path)
