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
from sync import Document, ElasticSearchConnection, CassandraConnection
```