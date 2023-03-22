# ---
# jupyter:
#   jupytext:
#     text_representation:
#       extension: .py
#       format_name: percent
#       format_version: '1.3'
#       jupytext_version: 1.14.5
#   kernelspec:
#     display_name: tsde_env
#     language: python
#     name: python3
# ---

# %% [markdown]
# Notebook 2: Analysis of the Connections Graph
# ==========

# %%
import networkx as nx
import pandas as pd

# %%
stations = pd.read_parquet('../data/processed/stations.parquet')
connections = pd.read_parquet('../data/processed/connections23031608.parquet')
connections.head(5)

# %%
stations.head()

# %% [markdown]
# # Making a graph
# There are a couple of possible ways to turn the data into a graph. 
# 1. Connect every station with the next (or previous) station on the path for all journeys
# 2. Connect every station only with the terminal (or first) station on the path
# 3. Connect every station directly with every station on the path
#
# The first option seems the most logical, but in that case I would not have the information about necessary train changes for a trip.
# If you want to go from Eisenberg to Eisenach, you would be able to see all the different stations on the path, but you would not 
# have any information about whether you would have to change the train at a specific station or not.
#
# The second option would solve this problem, but an important stations near an even more better connected station would be neglected.
#
# The third option would not have the problems I just explained, although this choice seems quite illogical, as the stations on a path 
# would not be connected with each other. 
#
# *Having information about travel times would give me more options, but unfortunately, the API doesn't give that much information per query.
# It would be possible to use the daily_trip_id to compute the travel times, but there would be a lot of missing values if I only make queries
# regarding a couple of hours per day.*
#
# I will therefore use a mixed approach: I will first connect every station with every station on the path (like in the 3rd option presented),
# but I will then prune the graph, keeping only the stations that are terminal stations for at least one trip.

# %%
G = nx.Graph()
for i,row in connections.iterrows():
    query_station = row['station_name']
    path = row['path']
    G.add_edges_from(list(zip([query_station]*len(path),path)))

# %% [markdown]
# The graph should be connected, since I started searching from one point (Siegen Hbf)

# %%
nx.is_connected(G)

# %%
print("Number of nodes:", G.number_of_nodes())
print("Number of edges:", G.number_of_edges())
print('Average degree (number of edges per node):', 2* G.number_of_edges()/G.number_of_nodes())

# %% [markdown]
# ## Minimum spanning tree (Kruskal's algorithm)

# %%
min_spanning_tree = nx.tree.minimum_spanning_tree(G)
min_spanning_tree.number_of_edges()

# %% [markdown]
# ## Average shortest path length

# %%
# Example: Eitorf to Eisenach
print(nx.shortest_path(G, source="Eitorf", target="Eisenach"))
print(nx.shortest_path_length(G, source="Eitorf", target="Eisenach"))
print(nx.average_shortest_path_length(G))

# %%
nx.find_cycle(G)

# %%
degrees = pd.Series(dict(G.degree()))

# %%
degrees.sort_values(ascending=False, inplace=True)
import seaborn as sns
sns.kdeplot(data=degrees[600:])


# %%
degrees.head(20)

# %% [markdown]
# It is important to keep in mind, that the importance of the connected stations is not represented in the degree of a stations.
# An ICE connection, for example, might connect the same number of stations to the station in question as a simple RB and will
# hence be valued equally.

# %% [markdown]
# ## Pruning of the graph
#

# %%
G_full = G.copy()


# %%
def filter_stations(station_id):
    if station_id not in connections['station_name'].to_list():
        return False
    elif station_name not in stations['name'].to_list():
        return False
    elif stations[stations['name']==station_name]['latitude'].isna()[0]:
        return False
    else:
        return True

subgraph = nx.subgraph_view(G, filter_node=filter_stations)

# %%
print(G.number_of_nodes())
print(subgraph.number_of_nodes())

# %%
pos = (
    stations.set_index('name')
    .apply(func=(lambda row: (row['latitude'], row['longitude'])), axis=1)
    .dropna()
    .to_dict()
)

# %%
stations.set_index('name')['eva'].dropna().isna().sum()

# %%
# For plotting, the labels have to be integers
station_names_to_eva_dict = (
    stations.set_index('name_normalized')['eva']
    .dropna()
    .astype('int')
    .to_dict()
    )

subgraph = nx.relabel_nodes(subgraph, mapping=station_names_to_eva_dict)

# %%
subgraph.nodes

# %%
for i in station_names_to_eva_dict.items():
    print(i)

# %%
from bokeh.palettes import Category20_20
from bokeh.plotting import figure, from_networkx, show

p = figure(x_range=(-2, 2), y_range=(-2, 2),
           x_axis_location=None, y_axis_location=None,
           tools="hover", tooltips="index: @index")
p.grid.grid_line_color = None

G = nx.convert_node_labels_to_integers(G)

graph = from_networkx(G, pos, scale=1.8, center=(0,0))
p.renderers.append(graph)

# Add some new columns to the node renderer data source
graph.node_renderer.data_source.data['index'] = list(range(len(G)))
graph.node_renderer.data_source.data['colors'] = Category20_20

graph.node_renderer.glyph.update(size=20, fill_color="colors")

show(p)
