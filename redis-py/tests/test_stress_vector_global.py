__author__ = 'sunlei 2014.08.04'
__doc__ = 'global variables and functions for stress test vector'
import random

keyrange = (1,2000000)
id_metarange = (1406870503888888 ,2000000)
schemarange = 1000
columnrange = 100
nreadThread = 5
nwriteThread = 2
host = '10.77.109.117'
port = 6379

#list of schema name(string)
schemas = []

#each item is schemaname(string):(columnname(string), columnlen(number))
columns = {}

def get_schema():
    return schemas[random.randrange(0, len(schemas))]