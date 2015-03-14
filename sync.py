#!/usr/bin/env python

import sys
import getopt
import datetime as dt
from time import sleep
import json
from elasticsearch import Elasticsearch
from cassandra.cluster import Cluster
from cassandra import InvalidRequest
import uuid

USAGE = """Usage: sync.py [OPTION] KEYSPACE:TABLE PERIOD

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

In ElasticSearch, field _timestamp must be enabled and stored, for the
index/doc_type of all of the database's documents. If the index and/or
type of a document to be inserted do not exist, they are created with
the mapping above (to enable and store _timestamp).

Options:
  -h, --help   display this help and exit
  -s           synchronize all existing data when the program starts [OFF]
  -v           run in verbose mode [OFF]
"""


def usage():
    """
    Prints usage
    """
    print USAGE


def get_opts():
    """
    Gets command line options, or prints usage and exits

    Returns a dict of options with keys: 'verbose', 'init_sync',
    'period', 'keyspace', 'table'
    """
    try:
        opts, args = getopt.getopt(sys.argv[1:], 'hvs', ['help'])
    except getopt.GetoptError as err:
        # print help information and exit:
        print str(err)
        usage()
        sys.exit(1)

    try:
        assert len(args) == 2
        keyspace, table = args[0].split(':')
        period = float(args[1])
    except:
        usage()
        sys.exit(1)

    verbose = False
    init_sync = False
    for o, _ in opts:
        if o == "-v":
            verbose = True
        elif o in ("-h", "--help"):
            usage()
            sys.exit()
        elif o == '-s':
            init_sync = True
        else:
            print "Unknown option {}".format(o)
            usage()
            sys.exit(1)

    return {'verbose': verbose,
            'init_sync': init_sync,
            'period': period,
            'keyspace': keyspace,
            'table': table}


class Document:
    """
    Class holding a document, i.e., metadata (index, type,
    id and timestamp) + content
    """
    def __init__(self, index, type_, content, id_=None, timestamp=None):
        """
        Initializes document with metadata (index, type, id, timestamp)
        and content. If no id is given, a uuid4 is automatically
        assigned. If no timestamp is given, the current time is used
        instead when inserting the document into a database.
        Content should be a Python dict.

        @type index: unicode
        @type type_: unicode
        @type id_: uuid.UUID
        @type timestamp: datetime.datetime
        @type content: dict
        """
        self.index = index
        self.type = type_
        self.content = content
        self.id = id_ if id_ else uuid.uuid4()
        self.timestamp = timestamp

    def __repr__(self):
        s = u"(Document)<id: {}, index: {}, type: {}, timestamp: {}>"
        return s.format(self.id, self.index, self.type, self.timestamp)

    def __str__(self):
        return u"{}\nContent:\n{}".format(repr(self), self.content)

    def insert_into_elasticsearch(self, es):
        """
        Inserts doc into ElasticSearch database, with explicit timestamp,
        which is document's timestamp if available, else current timestamp.
        If the document's index/type does not exist, we put a mapping
        to enable and store _timestamp.

        @type es: ElasticSearchConnection
        """

        if not es.connection.indices.exists_type(self.index, self.type):
            # doc type does not exists yet: creating a mapping
            # to enable and store timestamp
            if not es.connection.indices.exists(self.index):
                _ = es.connection.indices.create(self.index)
            body = {self.type: {"_timestamp": {"enabled": True, "store": True}}}
            _ = es.connection.indices.put_mapping(index=self.index,
                                                  doc_type=self.type,
                                                  body=body)

        # inserting document with explicit timestamp
        # (document's timestamp if given, else current time)
        timestamp = self.timestamp if self.timestamp else dt.datetime.now()
        _ = es.connection.index(id=self.id,
                                index=self.index,
                                doc_type=self.type,
                                body=self.content,
                                timestamp=timestamp)

    def insert_into_cassandra(self, cas):
        """
        Inserts doc into Cassandra database (serializing content
        to JSON format)

        @type cas: CassandraConnection
        """
        # inserting document with explicit timestamp
        # (document's timestamp if given, else current time)
        timestamp = self.timestamp if self.timestamp else dt.datetime.now()
        
        # content to JSON
        content = json.dumps(self.content)
        
        s = """
        insert into {}(index_, type, id, timestamp, content)
        values (%s, %s, %s, %s, %s)
        """
        cas.session.execute(s.format(cas.table),
                            [self.index, self.type, self.id, timestamp, content])


class AbstractConnection:
    """
    Abstract connection to database, defining method to implement in subclasses
    """
    def __init__(self, name):
        self.name = name

    def __repr__(self):
        return "<Connection to {}>".format(self.name)

    def get_documents(self, from_timestamp=None):
        """
        Returns documents, from a given timestamp.
        @rtype: list of Document
        """
        raise NotImplementedError("Subclass must implement method")

    def insert_document(self, document):
        """
        Inserts a document into the database
        @type document: Document
        """
        raise NotImplementedError("Subclass must implement method")

    def delete_docs_by_id(self, id_):
        """
        Deletes documents with given id
        """
        raise NotImplementedError("Subclass must implement method")

    def insert_or_replace_document(self, document):
        """
        Inserts a document into the database, after deleting all
        document(s) fo same id
        @type document: Document
        """
        self.delete_docs_by_id(document.id)
        self.insert_document(document)

    def sync_with(self, other, from_timestamp=None, verbose=False):
        """
        Synchronizes documents between this database and *other* database:
        - documents in current db but not in other db are inserted into other db
        - documents in other db but not in current db are inserted into current db
        - if a document is in both databases with different timestamps,
          the older is updated
        - if a document is in both databases with the same timestamp,
          we don't touch them

        Comparison between two documents is based on documents' id.

        @type other: AbstractConnection
        @type from_timestamp: datetime.datetime
        """
        # getting all documents [from timestamp]...
        if verbose and not from_timestamp:
            print "Querying all documents...",
        elif verbose and from_timestamp:
            print "Querying documents from {}...".format(from_timestamp),

        # ... from current db
        current_docs = self.get_documents(from_timestamp=from_timestamp)
        if verbose:
            print "[{}: got {}]".format(self.name, len(current_docs)),

        # ... from other db
        other_docs = other.get_documents(from_timestamp=from_timestamp)
        if verbose:
            print "[{}: got {}]".format(other.name, len(other_docs))

        # preparing lists of docs to insert or update:

        # docs in current db but not in other db (based on id)
        other_docids = [doc.id for doc in other_docs]
        current_docs_more_recent = [doc for doc in current_docs
                                    if not doc.id in other_docids]

        # docs in other db but not in current db (based on id)
        current_docids = [doc.id for doc in current_docs]
        other_docs_more_recent = [doc for doc in other_docs
                                  if not doc.id in current_docids]

        # docs in both databases:
        # ... loop on current docs also present in other db
        current_docs_in_other = [doc for doc in current_docs
                                 if not doc in current_docs_more_recent]
        for current_doc in current_docs_in_other:
            # ... corresponding other doc (based on id)
            other_doc = next(doc for doc in other_docs if doc.id == current_doc.id)
            if current_doc.timestamp == other_doc.timestamp:
                # both documents have the same timestamp: we won't
                # touch any of them (note that we don't check for content)
                continue
            elif current_doc.timestamp > other_doc.timestamp:
                # current doc is more recent
                current_docs_more_recent.append(current_doc)
            else:
                # other doc is more recent
                other_docs_more_recent.append(other_doc)

        # inserting or updating docs
        if current_docs_more_recent and verbose:
            s = "Inserting or updating {} documents into {}..."
            print s.format(len(current_docs_more_recent), other.name)
        for doc in current_docs_more_recent:
            other.insert_or_replace_document(doc)

        if other_docs_more_recent and verbose:
            s = "Inserting or upddating {} new documents into {}..."
            print s.format(len(other_docs_more_recent), self.name)
        for doc in other_docs_more_recent:
            self.insert_or_replace_document(doc)


class ElasticSearchConnection(AbstractConnection):
    """
    Class managing connection to ElasticSearch database
    """
    def __init__(self, *args, **kwargs):
        """
        Initializes connection to ElasticSearch
        """
        AbstractConnection.__init__(self, name='ElasticSearch')
        self.connection = Elasticsearch(*args, **kwargs)

    def get_documents(self, from_timestamp=None):
        """
        Returns all documents of the database HAVING THE FIELD _timestamp,
        from a given timestamp.
        @rtype: list of Document
        """
        # querying docs, with source and timestamp
        if not from_timestamp:
            body = {'filter': {'exists': {'field': '_timestamp'}}}
        else:
            body = {'filter': {'range': {'_timestamp': {'gte': from_timestamp}}}}
        res = self.connection.search(body=body, fields=['_source', '_timestamp'])
        docs = res['hits']['hits']

        # Example of docs:
        #
        # [{u'_id': u'4aa77aa4-06cd-48fd-b29d-d7a62725b31a',
        #   u'_index': u'test',
        #   u'_score': None,
        #   u'_source': {u'author': u'pouet'},
        #   u'_type': u'test',
        #   u'fields': {u'_timestamp': 1426116732093}},
        #  ...]

        documents = []
        for doc in docs:
            # converting timestamp from ms to datetime
            timestamp = dt.datetime.utcfromtimestamp(doc['fields']['_timestamp'] / 1000.0)
            document = Document(id_=uuid.UUID(str(doc['_id'])),  # string to uuid
                                index=doc['_index'],
                                type_=doc['_type'],
                                content=doc['_source'],
                                timestamp=timestamp)
            documents.append(document)

        return documents

    def insert_document(self, document):
        """
        Inserts a document into the database, with explicit timestamp,
        which is document's timestamp if available, else current timestamp.
        If the document's index/type does not exist, we put a mapping
        to enable and store _timestamp.
        """
        document.insert_into_elasticsearch(self)

    def delete_docs_by_id(self, id_):
        """
        Deletes documents with given id, whatever their index and type
        """
        body = {'query': {'term': {'_id': str(id_)}}}
        _ = self.connection.delete_by_query(index='_all', doc_type='', body=body)


class CassandraConnection(AbstractConnection):
    """
    Class managing connection to Cassandra table (i.e., column family)
    """
    def __init__(self, keyspace, table, *args, **kwargs):
        """
        Initializes connection to Cassandra table (i.e., column family).
        Creates *keyspace* and column family *table* if necessary, with
        fields:

        - id        = uuid
        - timestamp = timestamp
        - index_    = varchar
        - type      = varchar
        - content   = varchar

        and composite primary key (id, timestamp)
        """
        AbstractConnection.__init__(self, name='Cassandra')

        self.table = table  # Cassandra table

        # connecting to keyspace
        cluster = Cluster(*args, **kwargs)
        self.session = cluster.connect()
        try:
            self.session.set_keyspace(keyspace)
        except InvalidRequest:
            # creating keyspace
            s = """
            create keyspace {}
            with replication = {{'class': 'SimpleStrategy', 'replication_factor': 1}}
            """
            self.session.execute(s.format(keyspace))
            self.session.set_keyspace(keyspace)

        # creating table if necessary
        s = "select * from System.schema_columnfamilies where keyspace_name='{}';"
        rows = self.session.execute(s.format(keyspace))
        if all(r.columnfamily_name != table for r in rows):
            s = """
            create table {}(
              id uuid,
              timestamp timestamp,
              index_ varchar,
              type varchar,
              content varchar,
              primary key(id, timestamp))
            """
            self.session.execute(s.format(table))

    def get_documents(self, from_timestamp=None):
        """
        Returns documents of the database, from a given timestamp.
        @rtype: list of Document
        """
        if not from_timestamp:
            rows = self.session.execute("select * from {}".format(self.table))
        else:
            s = "select * from {} where timestamp >= %s allow filtering"
            rows = self.session.execute(s.format(self.table), [from_timestamp])

        # Example of response:
        #
        # [Row(id=UUID('c702e46f-0ec6-459a-a997-868bfaa20f78'),
        #      timestamp=datetime.datetime(2015, 3, 13, 23, 28, 12, 724000),
        #      index_=u'myindex',
        #      type=u'mytype',
        #      content=u'{"author": "nono"}'),
        #  ...]

        documents = []
        for row in rows:
            document = Document(index=row.index_,
                                type_=row.type,
                                id_=row.id,
                                timestamp=row.timestamp,
                                content=row.content)
            documents.append(document)

        return documents

    def insert_document(self, document):
        """
        Inserts doc into Cassandra database
        """
        document.insert_into_cassandra(self)

    def delete_docs_by_id(self, id_):
        """
        Deletes documents with given id from table
        """
        self.session.execute("delete from {} where id = %s".format(self.table), [id_])


if __name__ == '__main__':
    """
    Main program
    """
    # parsing command line options
    opts = get_opts()
    verbose = opts['verbose']
    init_sync = opts['init_sync']
    period = opts['period']
    cass_keyspace = opts['keyspace']
    cass_table = opts['table']

    # connecting to ElasticSearch on localhost (default port)
    if verbose:
        print "Connecting to ElasticSearch...",
    try:
        es = ElasticSearchConnection()
    except Exception as err:
        if verbose:
            print "[got error: {}]".format(err)
        sys.exit(1)
    else:
        if verbose:
            print "[ok]"

    # connecting to Cassandra on localhost (default port)
    if verbose:
        s = u"Connecting to Cassandra (keyspace '{}', table '{}')..."
        print s.format(cass_keyspace, cass_table),
    try:
        cass = CassandraConnection(keyspace=cass_keyspace, table=cass_table)
    except Exception as err:
        if verbose:
            print "[got error: {}]".format(err)
        sys.exit(1)
    else:
        if verbose:
            print "[ok]"

    # initial sync of all documents (if required)
    if init_sync:
        es.sync_with(cass, verbose=verbose)

    # syncing every *period* seconds
    last_sync_timestamp = dt.datetime.now()
    while True:
        sleep(period)
        now = dt.datetime.now()  # preparing timestamp before sync
        es.sync_with(cass, from_timestamp=last_sync_timestamp, verbose=verbose)
        last_sync_timestamp = now