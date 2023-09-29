from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
from cassandra.query import dict_factory
from cassandra.query import SimpleStatement
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

session.execute("DROP TABLE IF EXISTS airbnb_vectorized")

createtable = """CREATE TABLE airbnb_vectorized (
    city text,
    average_rate_per_night text,
    url text,
    bedrooms_count text,
    date_of_listing text,
    description text,
    title text,
    vectors vector<float, 384>,
    PRIMARY KEY (city, average_rate_per_night, url));"""

analyzerindex = """CREATE CUSTOM INDEX description_analyzer_index ON airbnb_vectorized(description)
    USING 'org.apache.cassandra.index.sai.StorageAttachedIndex' WITH OPTIONS = {
    'index_analyzer': '{
    "tokenizer" : {"name" : "standard"},
    "filters" : [{"name" : "porterstem"}]
    }'};"""

vectorindex = """CREATE CUSTOM INDEX vectors_vector_index ON airbnb_vectorized(vectors)
    USING 'org.apache.cassandra.index.sai.StorageAttachedIndex'
    WITH OPTIONS = { 'similarity_function': 'dot_product' }"""

#create your schema
session.execute(createtable)
session.execute(analyzerindex)
session.execute(vectorindex)

airbnb_raw = pd.read_csv('Airbnb_Texas_Rentals.csv')
model = SentenceTransformer('all-MiniLM-L6-v2', device='cpu')

insert = session.prepare(
    """INSERT INTO airbnb_vectorized
    (city, average_rate_per_night, url, bedrooms_count, 
    date_of_listing, description, title, vectors)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?)""")

for id, row in airbnb_raw.iterrows():
  ##Skip any row where description is null
  if pd.isna(row.description):
    continue
  emb = model.encode(row.description).tolist()

  ##Check if average_rate_per_night is NaN and make it $0.01
  if pd.isna(row.average_rate_per_night):
    row.average_rate_per_night = '$0.01'

  session.execute(insert, [row.city, row.average_rate_per_night, row.url, row.bedrooms_count, row.date_of_listing, row.description, row.title, emb])
  
