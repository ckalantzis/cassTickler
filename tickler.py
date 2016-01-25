import sys
import time
from cassandra.cluster import Cluster
from cassandra.query import SimpleStatement
from cassandra.metadata import protect_name
from cassandra import ConsistencyLevel

# Get the arguments
cass_keyspace = sys.argv[1]  # Cassandra Keyspace containing the Table to tickle
cass_table = sys.argv[2]  # Cassandra Table to tickle
cass_ip = sys.argv[3]  # Cassandra Port
cass_port = sys.argv[4]  # CQL Port
cass_throttle = sys.argv[5]  # microseconds

# print cass_keyspace  # debug
# print cass_table  # debug
# print cass_ip  # debug
# print cass_port  # debug
# print cass_throttle  # debug

# Set the connections to the cluster
cluster = Cluster(
    [cass_ip],
    port=cass_port, compression=False)

# Connect to the KS
session = cluster.connect(cass_keyspace)


# get the primary key of the table
# May have interesting behavior if the table has a composite PK
primary_key = cluster.metadata.keyspaces[cass_keyspace].tables[cass_table].primary_key[0].name
# print primary_key # debug

if primary_key:
    # read every key of the table
    query = 'SELECT id FROM ' + protect_name(cass_table)
    statement = SimpleStatement(query, fetch_size=1000, consistency_level=ConsistencyLevel.QUORUM)
    print 'Starting to repair table ' + cass_table
    repair_query = 'SELECT COUNT(1) FROM {} WHERE {} = ?'.format(protect_name(cass_table),
                                                                 protect_name(primary_key))
    repair_statement = session.prepare(repair_query)
    repair_statement.consistency_level = ConsistencyLevel.ALL
    row_count = 0
    for user_row in session.execute(statement):
        # print user_row.id  # debug
        row_count += 1
        session.execute(repair_statement, [user_row.id])
        time.sleep(float(cass_throttle) / 1000000)  # delay in microseconds between reading each row
        if (row_count % 1000) == 0:
            print str(row_count) + ' rows processed'
    print 'Repair of table ' + cass_table + ' complete'
    print str(row_count) + ' rows read and repaired'
