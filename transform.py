import pandas as pd
import re

# read data/data_*.json
data = []
df = pd.read_json("data/data_90866048.json", orient="records")


text_df = df["content"]
text_df = text_df.map(lambda x: x.replace("\n", ""))
# 연속된 공백 제거
text_df = text_df.map(lambda x: re.sub(r"\s+", " ", x))

print(text_df.head())

print(df["commentCount"].sum())
