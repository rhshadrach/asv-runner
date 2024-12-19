import json
import datetime as dt
import pyarrow as pa
import itertools as it
import pandas as pd
import os

input_filename = "results.json"
output_filename = "results.parquet"
results = json.load(open(input_filename))
commit_hash = results["commit_hash"]
columns = results["result_columns"]
buf = {"name": [], "params": [], "result": []}
for name, benchmark in results['results'].items():
    data = dict(zip(columns, benchmark))
    result = data["result"]
    params = list(it.product(*data["params"]))
    buf["name"].extend([name] * len(result))
    buf["params"].extend(params)
    buf["result"].extend(result)
buf["name"] = pd.array(buf["name"], dtype="string[pyarrow]")
buf["params"] = pd.array(buf["params"], dtype=pd.ArrowDtype(pa.list_(pa.string())))
buf["result"] = pd.array(buf["result"], dtype="float64[pyarrow]")
df = pd.DataFrame(buf)
df["date"] = pd.array([dt.datetime.today()] * len(df), dtype=pd.ArrowDtype(pa.timestamp("us")))
df["sha"] = pd.array([commit_hash] * len(df), dtype="string[pyarrow]")
df = df[["date", "sha", "name", "params", "result"]]

if os.path.exists("data/results.parquet"):
    existing = pd.read_parquet("data/results.parquet")
    final = pd.concat([existing, df])
else:
    final = df
final.to_parquet(f"data/{output_filename}")
