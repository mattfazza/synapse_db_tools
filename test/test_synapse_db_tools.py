import pytest
from src import synapse_db_tools as sdb
from sqlalchemy import create_engine, Table, Column, Integer, String, MetaData
import synapseclient

syn = synapseclient.Synapse()
syn.login(rememberMe=True)

engine = create_engine("mysql://synapse_db_tools:1985@localhost/",echo = True)
engine.execute("CREATE DATABASE IF NOT EXISTS synapse_projects;")
engine.execute("USE synapse_projects;")

def test_get_name(syn_obj = syn, project_id = "syn23652344"):
    result = sdb._get_project_name(syn_obj, project_id)
    assert result == "TREAD_AD_POC_0_4"

def test_get_project_tables(syn_obj = syn, project_id = "syn23652344"):
    result = sdb._get_project_tables(syn_obj, project_id)
    assert result == ['syn25177447', 'syn23667190', 'syn25328798']

def test_create_table(syn_obj = syn, table_with_pk = 'syn25328798', table_without_pk = 'syn23667190', eng=engine):
    result = sdb._create_table(syn, table_with_pk, eng)
    assert result == 1