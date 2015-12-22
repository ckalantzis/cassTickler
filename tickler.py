import sys
import time
from cassandra.cluster import Cluster
from cassandra.query import SimpleStatement

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
    port=cass_port, protocol_version=2, compression=False)

# Connect to the KS
session = cluster.connect(cass_keyspace)
print 'Connected to Cluster'

# Store existing read repair chance and set to 1
cass_table_schema = cluster.metadata.keyspaces[cass_keyspace].tables[cass_table]
orig_read_repair_chance = cass_table_schema.options.get('read_repair_chance')
print 'Original read repair chance value of ' + str(orig_read_repair_chance) + ' saved.'

session.execute('ALTER TABLE ' + cass_table + ' WITH read_repair_chance = 1;')
print 'Read repair chance set to 1'


# read every key of the table
query = "SELECT * FROM " + cass_table  # kv contains 1000000 rows
statement = SimpleStatement(query, fetch_size=1000)
print 'Starting to repair table ' + cass_table
row_count = 0
for user_row in session.execute(statement):
    # print user_row.id  # debug
    row_count += 1
    time.sleep(float(cass_throttle) / 1000000)  # delay in microseconds between reading each row
    if (row_count % 1000) == 0:
        print str(row_count) + ' rows processed'
print 'Repair of table ' + cass_table + ' complete'
print str(row_count) + ' rows read and repaired'


# Set read repair back to original value
session.execute('ALTER TABLE ' + cass_table + ' WITH read_repair_chance = ' + str(orig_read_repair_chance) + ';')
print 'Read repair chance set back to original value ' + str(orig_read_repair_chance)
