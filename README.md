# hybrid_search
## Create Astra Vector Database
Create a Vector Database in Astra with the keyspace "search". Create a token to connect and download the secure connect bundle. Add it to the main directory
## Run the embed script
Run python embed.py to create your table, analyzer index and vector index
## Run just vector search
Run python vector.py to run script for just vector search. Question(query) can be edited on line 29.
## Run the hybrid search
Run python hybrid.py to run script for vector search with a specific partition key and analyzer/term search
