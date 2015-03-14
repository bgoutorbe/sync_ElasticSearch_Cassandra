Script that synchronizes documents between an ElasticSearch and a Cassandra
database.

Usage
-----

Usage: `sync.py [OPTION] KEYSPACE:TABLE PERIOD`

Checks and synchronizes documents between ElasticSearch and Cassandra
databases (on localhost, with default ports) with a periodicity of PERIOD
seconds. 

Documents of ElasticSearch database are synchronized, whatever their 
index and type, with documents of KEYSPACE.TABLE in Cassandra database.

In Cassandra, TABLE must have the following fields: id (uuid), index_ (varchar),
type (varchar), timestamp (timestamp) and content (varchar) -- which
hold document metadata (id, index_, type) and content in JSON format.
If they do not exist, KEYSPACE and/or TABLE are created with the schema
above.

In ElasticSearch, field \_timestamp must be enabled and stored, for the 
index/doc_type of all of the database's documents. If the index and/or
type of a document to be inserted do not exist, they are created with
the mapping above (to enable and store \_timestamp).

Options:

  - -h, --help:  display this help and exit

  - -s: synchronize all existing data when the program starts [OFF]

  - -v: run in verbose mode [OFF]
  
Python
------
Documents can be created and inserted into databases from Python:

```python
from sync import Document, ElasticSearchConnection, CassandraConnectioen

# connection to ElasticSearch (on localhost, default port)
es = ElasticSearchConnection()

# connection to keyspace `mykeyspace`, table `mytable` 
# on  Cassandra (on localhost, default port)
cass = CassandraConnection('mykeyspace', 'mytable')

# new document, with automatic id (uuid4) and no timestamp
doc = Document(index='myindex', type_='mytype', content={'any': 'content'})

# inserting document into ElasticSearch/Cassandra with current timestamp
# (or replacing if id already exists)
es.insert_or_replace_document(doc)
cass.insert_or_replace_document(doc)

# document with explicit id and timestamp
import uuid
import datetime as dt
id_ = uuid.uuid4()
timestamp = dt.datetime.now()
doc = Document(index='myindex', 
               type_='mytype', 
               content={'any': 'content'}, 
               id_=id_, 
               timestamp=timestamp)
               
# document is inserted with its timestamp (instead of current timestamp)
es.insert_or_replace_document(doc)
cass.insert_or_replace_document(doc)

# just to check
doc  # returns, e.g.: (Document)<id: 852338dd-cf0e-4124-a1a5-75d42bc71819, index: myindex, type: mytype, timestamp: 2015-03-14 17:04:35.736699>

cass.get_documents()
es.get_documents()
# returns, e.g., [(Document)<id: e99ae7e7-3930-4181-a48f-b5f710683eee, index: myindex, type: mytype, timestamp: 2015-03-14 17:02:40.301000>]
```