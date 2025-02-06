from __future__ import annotations

import argparse
import datetime as dt
import itertools as it
import json
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
    with open(input_path / "results" / "benchmarks.json") as fh:
        data = json.load(fh)
    benchmark_to_param_names = {
        k: v["param_names"] for k, v in data.items() if k != "version"
    }

    result_path = input_path / "results" / "asvrunner"
    buf: dict[str, list] = {
        "date": [],
        "sha": [],
        "name": [],
        "params": [],
        "result": [],
    }
    for result_json in result_path.glob("*.json"):
        if result_json.name == "machine.json":
            continue
        with open(result_json) as fh:
            results = json.load(fh)
        commit_hash = results["commit_hash"]
        columns = results["result_columns"]

        timestamp = dt.datetime.fromtimestamp(results["date"] / 1000)
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
            buf["date"].extend([timestamp] * len(result))
            buf["sha"].extend([commit_hash] * len(result))

    buf["name"] = pd.array(buf["name"], dtype="string[pyarrow]")
    buf["params"] = pd.array(buf["params"], dtype="string[pyarrow]")
    buf["result"] = pd.array(buf["result"], dtype="float64[pyarrow]")
    buf["date"] = pd.array(buf["date"], dtype=pd.ArrowDtype(pa.timestamp("us")))
    buf["sha"] = pd.array(buf["sha"], dtype="string[pyarrow]")
    df = pd.DataFrame(buf)

    columns = ["date", "sha", "name", "params", "result"]
    result = df[columns]

    parquet_path = output_path / "results.parquet"
    result = detect_regression(result, window_size=21)
    result.to_parquet(parquet_path)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--input-path")
    parser.add_argument("--output-path")
    args = parser.parse_args()
    run(input_path=args.input_path, output_path=args.output_path)
