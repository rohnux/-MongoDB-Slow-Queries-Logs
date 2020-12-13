from pymongo import MongoClient
from elasticsearch import Elasticsearch
import datetime
from bson import json_util
import json
from bson.json_util import dumps

indexName = "mongo-slow-query-" + datetime.date.today().strftime('%Y-%m-%d')

def connectElasticSearch():
    return Elasticsearch(['172.24.123.53'], port=9200)

uri = "mongodb://mongo-admin:password@1234@mongodb-1.digital.com:27017/?authSource=admin"
demo_client = MongoClient(uri)

#getting the list of all db
dblist = demo_client.list_database_names()

for db in dblist:
     demo_db = demo_client[db]
     demo_coll= demo_db["system.profile"]
     demo_doc=demo_coll.find()
     es = connectElasticSearch()
     for each_doc in demo_doc:
#    for debugging- import pdb;pdb.set_trace() 
	 if each_doc.get('command'):
              if each_doc.get('command').get('$clusterTime'):
                 each_doc.pop('command')['$clusterTime']
         if each_doc.get('originatingCommand'):
              if each_doc.get('originatingCommand').get('$clusterTime'):
                 each_doc.pop('originatingCommand')['$clusterTime']
	 result = each_doc.get('ts')
         if datetime.datetime.now() - result < datetime.timedelta(minutes=30):  #slow query of last 30 minutes
	      esResponse = es.index(index=indexName,body=each_doc)
	      print each_doc
            # print dumps(each_doc)
              print each_doc.get('ts')
