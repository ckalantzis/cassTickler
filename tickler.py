import string, sys, time
from cassandra.cluster import Cluster
from cassandra.query import SimpleStatement
import time


# Get the arguments
cassKeyspace = sys.argv[1]
cassTable = sys.argv[2]
cassIP = sys.argv[3]
cassPort = sys.argv[4]

print cassKeyspace
print cassTable
print cassIP
print cassPort



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

session.execute('ALTER TABLE ' + cassTable + ' WITH read_repair_chance = 1;')



# read every key of the table
query = "SELECT " + getpkname(cassTable) + " FROM " + cassTable + " limit 1000"  # kv contains 1000000 rows
statement = SimpleStatement(query, fetch_size=100)
for user_row in session.execute(statement):
    print user_row.id
    time.sleep(1)  # delays for 1 seconds


raise SystemExit(1)
