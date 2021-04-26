from sqlalchemy import create_engine, Table, Column, Integer, String, MetaData
from sqlalchemy.types import VARCHAR, INTEGER
from sqlalchemy.sql import text
import json
import pandas as pd
import synapseclient
from pathlib import Path
import sys
import numpy as np


# TO DO: replace with config file
def _engine_create(connection, schema_name):
    
    # connection_with_schema = connection + schema_name
    engine = create_engine(connection, encoding = 'utf-8', echo = True)

    create_query = str("CREATE SCHEMA IF NOT EXISTS {0};".format(schema_name))
    engine.execute(create_query)

    connection_with_schema = connection + schema_name + '?charset=utf8'
    print(connection_with_schema)
    engine = create_engine(connection_with_schema, echo = True)

    return engine

def _get_project_name(syn_obj, project_id):
    #do some tests and add try/catch
    return syn_obj.get(project_id).name

# def _create_database(eng, name):
    
    

def _get_project_tables(syn_obj, project_id):
    
    children_entities = syn_obj.getChildren(project_id)
    table_entities = filter(lambda child: child['type']=='org.sagebionetworks.repo.model.table.TableEntity', children_entities)
    table_ids = map(lambda table: table['id'], table_entities)
    
    if not table_ids:
        sys.exit("There are no tables in this project to be converted.")

    return list(table_ids)

def _create_table(syn_obj, table_id, eng):
    
    synapse_table = syn_obj.get(table_id)
    table_name = synapse_table['name'].replace(' ', '_')

    query = str("select * from {0}".format(table_id))
    query_result = syn_obj.tableQuery(query)
    result_as_df = query_result.asDataFrame()

    # result_as_df = pd.DataFrame(result_as_df, encoding = 'utf-8')
    result_as_df = result_as_df.replace([np.inf, -np.inf], np.nan)
    result_as_df = result_as_df.fillna(0)


    if 'pk' in synapse_table:
        # create positioning the index
        result_as_df.to_sql(table_name, if_exists='replace', con=eng, index=False, index_label=synapse_table['pk'])

        primary_key = synapse_table['pk'][0]
    else:
        artificial_index = table_name + "_id"

        result_as_df[artificial_index] = result_as_df.reset_index().index

        print(type(result_as_df))

        result_as_df.to_sql(table_name, if_exists='replace', con=eng, index=False, index_label=artificial_index)
        
        primary_key = artificial_index
    
    # add_pk_statement = str("ALTER TABLE {0} ADD PRIMARY KEY({1})".format(table_name, primary_key))
    # eng.execute(add_pk_statement)

    return 1


# Add the ability to pass in an array of Syn_ids
def create_project_database(connection_params, project_id, syn_obj=None):

    if syn_obj is None:
        syn = synapseclient.Synapse()
        syn.login(rememberMe=True)
    else:
        syn = syn_obj

    tables = _get_project_tables(syn_obj = syn, project_id = project_id)

    project_name = _get_project_name(syn_obj = syn, project_id = project_id).lower().replace(' ', '_').replace('-', '_')

    engine = _engine_create(connection = connection_params, schema_name = project_name)

    for table in tables:
        _create_table(syn_obj = syn, table_id = table, eng = engine)
    
    return "Successfully created database."


def main():
    username = sys.argv[1]
    password = sys.argv[2]
    host = sys.argv[3]
    project = sys.argv[4]

    connection = str("mysql://{0}:{1}@{2}/".format(username, password, host))
    create_project_database(connection_params = connection, project_id = project)


if __name__ == "__main__":
    main()
