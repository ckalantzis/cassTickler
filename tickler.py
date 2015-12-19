import string, sys, time
from cassandra.cluster import Cluster
from cassandra.query import SimpleStatement

# Get the arguments
cassKeyspace = sys.argv[1] # Cassandra Keyspace containing the Table to tickle
cassTable = sys.argv[2] # Cassandra Table to tickle
cassIP = sys.argv[3] # Cassandra Port
cassPort = sys.argv[4] # CQL Port
cassThrottle = sys.argv[5] # microseconds

print cassKeyspace
print cassTable
print cassIP
print cassPort
print cassThrottle

# Set the connections to the cluster
cluster = Cluster(
    [cassIP],
    port=cassPort, protocol_version=2, compression=False)

# Connect to the KS
session = cluster.connect(cassKeyspace)

# get the primary key of the table
###### This is dirty. May fail if the table has a composite PK
def getpkname(cassTableName):
    for yo in cluster.metadata.keyspaces[cassKeyspace].tables[cassTable].primary_key:
        primaryKey=yo.name
    return primaryKey
print getpkname(cassTable)


# Store existing read repair chance and set to 1
print cluster.metadata.keyspaces[cassKeyspace].tables[cassTable].export_as_string()
session.execute('ALTER TABLE ' + cassTable + ' WITH read_repair_chance = 1;')


# read every key of the table
query = "SELECT * FROM " + cassTable + " limit 1000"  # kv contains 1000000 rows
statement = SimpleStatement(query, fetch_size=100)
for user_row in session.execute(statement):
    print user_row.id
    time.sleep(float(cassThrottle) / 1000000)  # delay in microseconds between reading each row


# raise SystemExit(1)
