import pandas as pd 
import numpy as np 
import neo4j


##import seaborn as sns
import warnings
from neo4j import GraphDatabase
warnings.filterwarnings('ignore')
import networkx as nx
from networkx.algorithms.link_analysis.pagerank_alg import pagerank
from collections import Counter
##from pandas_profiling import ProfileReport


def iniciador():
    url = "neo4j+s://9d0eb28d.databases.neo4j.io"
    username = "neo4j"
    password = "ZsZO3ya5YB8EcmVwmxYEqn6Amj_Z7R4t_6VDU5oMIeI"
    driver = GraphDatabase.driver(url, auth=(username, password))
    with driver.session() as session:
        result = session.run("MATCH (n) RETURN count(n)")
        count = result.single()[0]
        print(f"La conexión con Aura Neo4j se ha establecido correctamente. Hay {count} nodos en la base de datos.")
    return driver
def _peliculas_cliente(tx, password):
    query = """
     MATCH (u:User {p : $id})-[:IN_VISTAS]->(m:Movie)-[:IN_GENRE]->(n:Genre)<-[:IN_GENRE]-(m2:Movie)
        where m.id <> m2.id
        return u.id, m.nombre, m2.nombre, n.nombre
        """ 
    result = tx.run(query, id=password)
    return list(result)
def peliculas_vistas(password):
    records = []
    with driver.session() as session:
        result = session.read_transaction(_peliculas_cliente, password)
        for record in result:
            records.append(record)
    print("Found  {record} records ".format(record=len(records )))

    return records

def create_networkx_graph(password):
    
    val_count  = Counter([(res['m.nombre'],res['m2.nombre']) for res in peliculas_vistas(password) ])
    if len(val_count)==0:
        print(f" no records for {password}")
    print(f"  no of  records for {password} {len(val_count)}")

    recs = pd.DataFrame([[a[0], a[1], val_count[a]] for a in val_count])

    G = nx.DiGraph()
    #recs= recs.head(20)
    total = len(val_count)
    for idx , rec in recs.iterrows():
        #print(f"{idx} of {total} loaded")
        G.add_edge(rec[0], rec[1], weight = rec[2])
    return G

def run_page_rank(G):
    pr=pagerank(G)
    sorted_pr = {k: v for k, v in sorted(pagerank(G).items(), key=lambda item: item[1], reverse = True)}
    df= pd.DataFrame(sorted_pr.items())
    df['rank'] = df[1].rank(method='dense', ascending=False)
    print(f"done calculating page rank")
    return df

def orquestador(password):
     driver=iniciador()
     G= create_networkx_graph(password =password)
     df=run_page_rank(G)
     df.head(10)