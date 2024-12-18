import json
import subprocess
from pathlib import Path
import datetime as dt
import pyarrow as pa
import itertools as it
import pandas as pd

# asv machine --machine=asvrunner --yes
# asv run --durations=30 --python=same --set-commit-hash=$(git rev-parse HEAD) --show-stderr --machine=asvrunner -b ^groupby.GroupByCythonAgg

base_path = "pandas/asv_bench"
machine = "asvrunner"
response = subprocess.run(f"cd {base_path} && git rev-parse HEAD", shell=True, capture_output=True)
githash = response.stdout.decode().strip()
short_githash = githash[:8]
paths = list(Path(f"{base_path}/results/{machine}/").glob(f"{short_githash}-existing*.json"))
assert len(paths) == 1
input_filename = paths[0]
output_filename = "results.parquet"
results = json.load(open(input_filename))
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
df["sha"] = pd.array([githash] * len(df), dtype="string[pyarrow]")
df = df[["date", "sha", "name", "params", "result"]]

# # TODO: Read from current branch
# existing = pd.read_parquet("results.parquet")
# final = pd.concat([existing, df])
final = df
final.to_parquet(output_filename)
