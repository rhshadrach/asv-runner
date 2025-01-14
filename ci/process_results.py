from __future__ import annotations

import argparse
import datetime as dt
import itertools as it
import json
import os
from pathlib import Path

import pandas as pd
import pyarrow as pa


def detect_regression(data: pd.DataFrame, window_size: int = 21) -> pd.DataFrame:
    data = (
        data[data["result"].notnull()]
        .set_index(["name", "params", "date"])
        .sort_index()
    )
    keys = ["name", "params"]
    tol = 0.95

    data["established_worst"] = (
        data.groupby(keys, as_index=False)["result"]
        .rolling(window_size, center=True)
        .max()[["result"]]
    )
    data["established_best"] = (
        data.groupby(keys, as_index=False)["result"]
        .rolling(window_size, center=True)
        .min()[["result"]]
    )

    mask = (
        # TODO: is the arg to shift right?
        data["established_worst"].groupby(keys).shift(window_size)
        < tol * data["established_best"]
    )
    mask = mask & ~mask.groupby(keys).shift(1, fill_value=False)
    mask = mask.groupby(keys).shift(-(window_size - 1) // 2, fill_value=False)

    data["is_regression"] = mask
    data["pct_change"] = data.groupby(keys)["result"].pct_change()
    data["abs_change"] = data["result"] - data.groupby(keys)["result"].shift(1)
    return data.reset_index()


def run(input_path: str | Path, output_path: str | Path):
    if not isinstance(input_path, Path):
        input_path = Path(input_path)
    if not isinstance(output_path, Path):
        output_path = Path(output_path)
    with open(input_path / "benchmarks.json") as fh:
        data = json.load(fh)
    benchmark_to_param_names = {
        k: v["param_names"] for k, v in data.items() if k != "version"
    }

    with open(input_path / "results.json") as fh:
        results = json.load(fh)
    commit_hash = results["commit_hash"]
    columns = results["result_columns"]
    buf: dict[str, list] = {"name": [], "params": [], "result": []}
    for name, benchmark in results["results"].items():
        data = dict(zip(columns, benchmark))
        result = data["result"]
        param_names = benchmark_to_param_names[name]
        params = [
            ", ".join(f"{k}={v}" for k, v in zip(param_names, e))
            for e in it.product(*data["params"])
        ]
        buf["name"].extend([name] * len(result))
        buf["params"].extend(params)
        buf["result"].extend(result)
    buf["name"] = pd.array(buf["name"], dtype="string[pyarrow]")
    buf["params"] = pd.array(buf["params"], dtype="string[pyarrow]")
    buf["result"] = pd.array(buf["result"], dtype="float64[pyarrow]")
    df = pd.DataFrame(buf)
    timestamp = dt.datetime.fromtimestamp(results["date"] / 1000)
    df["date"] = pd.array(
        [timestamp] * len(df), dtype=pd.ArrowDtype(pa.timestamp("us"))
    )
    df["sha"] = pd.array([commit_hash] * len(df), dtype="string[pyarrow]")

    columns = ["date", "sha", "name", "params", "result"]
    df = df[columns]

    parquet_path = output_path / "results.parquet"
    if os.path.exists(parquet_path):
        existing = pd.read_parquet(parquet_path)[columns]
        result = pd.concat([existing, df])
    else:
        result = df
    result = detect_regression(result, window_size=21)
    result.to_parquet(parquet_path)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--input-path")
    parser.add_argument("--output-path")
    args = parser.parse_args()
    run(input_path=args.input_path, output_path=args.output_path)
