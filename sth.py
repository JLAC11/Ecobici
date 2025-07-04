# %%
import pandas as pd
import numpy as np
import seaborn as sns
import matplotlib.pyplot as plt


sns.set_style("darkgrid")
df = pd.read_parquet("data/ecobici_data.parquet")
df.head()
# %%
df["duration_mins"] = (df["arribo"] - df["retiro"]).dt.total_seconds() / 60
df["duration_mins"].describe()
# %%
df.shape
# %%
df.groupby(pd.Grouper(key="arribo", freq="ME")).count().plot()
# %%
