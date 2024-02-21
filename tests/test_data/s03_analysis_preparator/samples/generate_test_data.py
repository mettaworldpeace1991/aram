import pandas as pd
import time
import random

start_date = "2024-01-01 00:00:01"
pattern = "%Y-%m-%d %H:%M:%S"
epoch_start = int(time.mktime(time.strptime(start_date, pattern)))

data = {
    "timeStamp": [],
    "elapsed": [],
    "label": [],
    "responseCode": [],
    "threadName": [],
    "success": [],
    "Latency": [],
    "Connect": [],
}

for i in range(5):  # 5 intervals
    timestamp = epoch_start + i * 300000  # Increment by 5 minutes
    elapsed_label1 = 1000  # Starting elapsed time for label1
    elapsed_label2 = 100  # Starting elapsed time for label2

    for j in range(10):  # 10 rows for label1
        data["timeStamp"].append(timestamp)
        data["elapsed"].append(elapsed_label1)
        data["label"].append("sample_label1")
        data["responseCode"].append(200)
        data["threadName"].append(f"t1")
        data["success"].append(True)
        data["Latency"].append(random.randint(0, 100))
        data["Connect"].append(random.randint(0, 50))
        elapsed_label1 += 100  # Increment elapsed time for each row

    for j in range(5):  # 5 rows for label2
        data["timeStamp"].append(timestamp)
        data["elapsed"].append(elapsed_label2)
        data["label"].append("sample_label2")
        data["responseCode"].append(404)
        data["threadName"].append(f"t2")
        data["success"].append(False)
        data["Latency"].append(random.randint(0, 100))
        data["Connect"].append(random.randint(0, 50))
        elapsed_label2 += 10  # Increment elapsed time for each row

df = pd.DataFrame(data)

jtl_file_path = "synthetic_jtl.csv"
fetcher_file_path = "synthetic_jtl.fetcher"

df.to_csv(jtl_file_path, index=False)
df.to_feather(fetcher_file_path)
