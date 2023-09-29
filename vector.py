from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
import os
from dotenv import load_dotenv
import pandas as pd
from sentence_transformers import SentenceTransformer

load_dotenv()

#Astra DB token
astratoken = os.getenv('ASTRA_TOKEN')
#Change your keyspace name if needed
my_ks = 'search'
#Astra Secure Connect Bundle file name
scbpath = os.getenv('SCB_FILE')

#Connect to Astra
cloud_config= {
  'secure_connect_bundle': scbpath
}
auth_provider = PlainTextAuthProvider('token', astratoken)
cluster = Cluster(cloud=cloud_config, auth_provider=auth_provider, protocol_version=4)
session = cluster.connect()
session.set_keyspace(my_ks)
print(session)

model = SentenceTransformer('all-MiniLM-L6-v2', device='cpu')
query = "I want an affordable place close to downtown"
queryembedded = model.encode(query).tolist()
astraquery = session.execute(
    f"""
    SELECT average_rate_per_night, city, description
    FROM airbnb_vectorized ORDER BY vectors ANN OF {queryembedded} LIMIT 25
    """)
results = pd.DataFrame(astraquery)
pd.set_option('display.expand_frame_repr', False)
pd.options.display.max_colwidth = 0
print(results)