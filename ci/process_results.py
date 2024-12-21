import json
import datetime as dt
import pyarrow as pa
import itertools as it
import pandas as pd
import os
import argparse
from pathlib import Path

def run(input_path: str, output_path: str | Path):
    if not isinstance(output_path, Path):
        output_path = Path(output_path)
    results = json.load(open(input_path))
    commit_hash = results["commit_hash"]
    columns = results["result_columns"]
    buf = {"name": [], "params": [], "result": []}
    for name, benchmark in results['results'].items():
        data = dict(zip(columns, benchmark))
        result = data["result"]
        params = [", ".join(e) for e in it.product(*data["params"])]
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

    if os.path.exists(output_path / "results.parquet"):
        existing = pd.read_parquet(output_path / "results.parquet")
        final = pd.concat([existing, df])
    else:
        final = df
    final.to_parquet(output_path)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--input-path")
    parser.add_argument("--output-path")
    args = parser.parse_args()
    run(input_path=args.input_path, output_path=args.output_path)
