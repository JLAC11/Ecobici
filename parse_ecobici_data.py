# %%
import pandas as pd
import numpy as np
import seaborn as sns
import matplotlib.pyplot as plt
import json
from scipy.cluster import hierarchy
from scipy.spatial import distance
from collections import defaultdict
import networkx as nx

plt.style.use("styles/ecobici.mplstyle")

# %%
df = pd.read_csv(
    "data/historic/ecobici_2025-05.csv",
    dtype={
        "Genero_Usuario": "str",
        "Edad_Usuario": "float",
    },
)
df["momento_retiro"] = pd.to_datetime(
    df["Fecha_Retiro"] + " " + df["Hora_Retiro"], format="%d/%m/%Y %H:%M:%S"
)
df["momento_arribo"] = pd.to_datetime(
    df["Fecha_Arribo"] + " " + df["Hora_Arribo"], format="%d/%m/%Y %H:%M:%S"
)
df["duration"] = df["momento_arribo"] - df["momento_retiro"]
df["duration"] = pd.to_timedelta(df["duration"])
df.rename(
    columns={"Ciclo_EstacionArribo": "destino", "Ciclo_Estacion_Retiro": "origen"},
    inplace=True,
)
stations = pd.DataFrame(json.load(open("data/station_info.json"))["data"]["stations"])
stations.sort_values(by="capacity", ascending=False).head(20)[
    ["name", "capacity"]
].to_markdown("generated/assets/top_20_stations.md", index=False, tablefmt="github")
stationsnames = stations[["name", "short_name"]]

df["origen"] = df["origen"].map(stationsnames.set_index("short_name")["name"])
df["destino"] = df["destino"].map(stationsnames.set_index("short_name")["name"])
df["duration_mins"] = df["duration"].dt.total_seconds() / 60
durationfilter = df["duration"] <= df["duration"].quantile(0.995)
print(df.head())

df.groupby("origen")["Bici"].count().sort_values(ascending=False).head(20).to_markdown(
    "data/top_20_stations_origins.md"
)
df.groupby("destino")["Bici"].count().sort_values(ascending=False).head(20).to_markdown(
    "data/top_20_stations_destinations.md"
)

print(f"Unique stations: {len(df['origen'].unique())}")


# %%
def quantile_90(x):
    return x.quantile(0.90)


w = (
    df.groupby(df["momento_retiro"].dt.hour)
    .agg(
        {
            "Bici": "count",
            "duration_mins": ["mean", "min", "max", "median", quantile_90],
        }
    )
    .reset_index()
    .astype({"Bici": "int"})
    .rename(columns={"momento_retiro": "hora"})
)
w.to_markdown(
    "data/usage_by_hour.md", index=False, tablefmt="github", intfmt=",", floatfmt=",.2f"
)
sns.barplot(data=w, x="hora", y=("Bici", "count"), hue=("duration_mins", "mean"))
plt.title("Bikes used by hour")
plt.xlabel("Hour of the day")
plt.ylabel("Number of bikes used in the month")
plt.legend(title="Average duration (minutes)")
plt.gcf().set_size_inches(12, 6)
plt.tight_layout()
plt.savefig("generated/assets/usage_by_hour.png")
# %%

w = df.groupby([df["momento_retiro"].dt.weekday, df["momento_retiro"].dt.hour])[
    "Bici"
].count()
w.index = w.index.set_names(["weekday", "hour"])
w = w.unstack("weekday")
ax = sns.heatmap(
    data=w,
    cmap="Blues",
    annot=False,
    fmt="d",
)

ax.set_xlabel("Day of the week")
ax.set_ylabel("Hour of the day")
ax.set_title("Bikes used by weekday and hour")
ax.set_xticklabels(
    ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"],
    rotation=45,
)
ax.set_yticklabels(
    [f"{i}:00" for i in range(24)],
    rotation=0,
)
plt.gcf().set_size_inches(12, 6)
plt.xticks(rotation=45)
plt.yticks(rotation=0)
plt.xlabel("Day of the week")
plt.ylabel("Hour of the day")

plt.tight_layout()
plt.savefig("generated/assets/usage_by_weekday_hour.png")
# %% Routes
values = (
    df.groupby(["origen", "destino"])["Bici"]
    .count()
    .sort_values(ascending=False)
    .reset_index()
)
print(values.head(20))
values.head(20).to_markdown(
    "generated/assets/top_20_routes.md",
    index=True,
    tablefmt="github",
    intfmt=",",
    floatfmt=",",
)
plt.style.use("styles/ecobici.mplstyle")
sns.ecdfplot(values, x="Bici", log_scale=(False, False), stat="percent")
plt.title("Cumulative distribution of bike routes")
plt.xlabel("Number of bikes used in the route")
plt.ylabel("Cumulative distribution (%)")
plt.xlim(-2, 200)
plt.ylim(0, 105)
plt.hlines(100, -2, 1000, linestyles="dashed", colors="#53231a", alpha=0.5)
plt.gcf().set_size_inches(12, 8)
plt.savefig("generated/assets/travelpairs-distribution.png")
# plt.tight_layout()

# %%
durationfilter = df["duration_mins"] <= df["duration_mins"].quantile(0.995)

sns.kdeplot(
    df[durationfilter],
    x="duration_mins",
    fill=True,
    common_norm=False,
)
plt.title("Duration of bike trips")
plt.xlabel("Duration (minutes)")
plt.ylabel("Density")
plt.vlines(45, 0, 1, linestyles="dashed", colors="#53231a", alpha=0.5)
plt.ylim(0, 0.065)
plt.tight_layout()
plt.savefig("generated/assets/travel_duration_distribution.png")

# %% Check trips by day-half

midday = df["momento_retiro"].dt.hour < 12

df.groupby([midday, df["origen"]])["Bici"].count().sort_values(ascending=False).head(
    20
).to_markdown(
    "generated/assets/top_20_origin_stations_by_halfday.md",
    index=True,
    tablefmt="github",
)

midday = df["momento_arribo"].dt.hour < 12

df.groupby([midday, df["destino"]])["Bici"].count().sort_values(ascending=False).head(
    20
).to_markdown(
    "generated/assets/top_20_destination_stations_by_halfday.md",
    index=True,
    tablefmt="github",
)

# %%
matr = df.pivot_table(index="origen", columns="destino", values="Bici", aggfunc="count")
matr = matr.dropna(axis=0, how="all")
matr = matr.dropna(axis=1, how="all")
matr = matr.fillna(0)
k = 5_000
submat = matr.loc[(matr.sum(axis=1)) > k, (matr.sum(axis=0)) > k]
# submat = submat / submat.sum(axis=0)
# submat = submat.T
eigvals, eigvecs = np.linalg.eig(matr)
print(f"Eigenvalue importance: {eigvecs[0]}")

fig = sns.heatmap(submat, cbar=True, cmap="Blues", annot=False)
plt.savefig("generated/assets/heatmap_routes.png")

# %%
sns.histplot(df, x="Edad_Usuario", binwidth=2, hue="Genero_Usuario")
plt.show()

# %%
matr.columns
# %%
df.groupby("origen")["Bici"].count().sort_values(ascending=False).plot().ecdf(
    x="origen"
)
# %%

pd.Series(
    nx.eigenvector_centrality(nx.from_pandas_adjacency(matr), max_iter=1000)
).sort_values(ascending=False).head(20)

# %%
df.groupby("origen").agg(
    {"duration": ["mean", "min", "max", "median", lambda x: x.quantile(0.90)]}
)["duration"].reset_index().sort_values(by="median", ascending=False)


# %%
def quantile_90(x):
    return x.quantile(0.90)


df.groupby("origen").agg({"duration": ["mean", "min", "max", "median", quantile_90]})[
    "duration"
].reset_index().sort_values(by="mean", ascending=False)
# %%
# Generate a stochastic block model graph
graph = nx.from_pandas_adjacency(matr)

# Analyze the graph (e.g., using community detection algorithms)
communities = list(nx.community.label_propagation_communities(graph))

# Print the communities
print("Communities:", communities)
# %%


def create_hc(G):
    """Creates hierarchical cluster of graph G from distance matrix"""
    path_length = nx.all_pairs_shortest_path_length(G)
    distances = np.zeros((len(G), len(G)))
    for u, p in path_length:
        for v, d in p.items():
            distances[u][v] = d
    # Create hierarchical cluster
    Y = distance.squareform(distances)
    Z = hierarchy.complete(Y)  # Creates HC using farthest point linkage
    # This partition selection is arbitrary, for illustrative purposes
    membership = list(hierarchy.fcluster(Z, t=1.15))
    # Create collection of lists for blockmodel
    partition = defaultdict(list)
    for n, p in zip(list(range(len(G))), membership):
        partition[p].append(n)
    return list(partition.values())


G = graph
# Extract largest connected component into graph H
H = G.subgraph(next(nx.connected_components(G)))
# Makes life easier to have consecutively labeled integer nodes
H = nx.convert_node_labels_to_integers(H)
# Create partitions with hierarchical clustering
partitions = create_hc(H)
# Build blockmodel graph
BM = nx.quotient_graph(H, partitions, relabel=True)

# Draw original graph
pos = nx.spring_layout(H, iterations=100, seed=83)  # Seed for reproducibility
plt.subplot(211)
nx.draw(H, pos, with_labels=False, node_size=10)

# Draw block model with weighted edges and nodes sized by number of internal nodes
node_size = [BM.nodes[x]["nnodes"] * 10 for x in BM.nodes()]
edge_width = [(2 * d["weight"]) for (u, v, d) in BM.edges(data=True)]
# Set positions to mean of positions of internal nodes from original graph
posBM = {}
for n in BM:
    xy = np.array([pos[u] for u in BM.nodes[n]["graph"]])
    posBM[n] = xy.mean(axis=0)
plt.subplot(212)
nx.draw(BM, posBM, node_size=node_size, width=0.1, with_labels=True)
plt.axis("off")
plt.show()
# %%
