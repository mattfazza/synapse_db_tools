from sqlalchemy import create_engine, Table, Column, Integer, String, MetaData
from sqlalchemy.types import VARCHAR, INTEGER
import json
import pandas as pd
import synapseclient

# TO DO: replace with config file
def _engine_create():
    return create_engine("mysql://synapse_db_tools:1985@localhost/synapse_projects",echo = False)


engine = _engine_create()


syn = synapseclient.Synapse()
syn.login(rememberMe=True)

results = syn.tableQuery("select * from {0}".format("syn23751446"))
table_data = results.asDataFrame()

table_data['artificial_index'] = table_data.reset_index().index

table_data.to_sql('gene_related_files', if_exists='replace', con=engine, index=False, index_label='row_num')

con.execute('ALTER TABLE gene_related_files ADD PRIMARY KEY(artificial_index)')

# df = pd.DataFrame({'name' : ['User 1', 'User 2', 'User 3']})
# df.to_sql('users', con=engine)


# def set_up_connection():
    
#     connection = engine.connect()
#     return connection


