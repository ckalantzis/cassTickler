import random, string, sys
from cassandra.cluster import Cluster
from cassandra.policies import DCAwareRoundRobinPolicy
from cassandra.policies import TokenAwarePolicy
from cassandra.query import SimpleStatement
from cassandra import ConsistencyLevel
from random import randint

raise SystemExit(1)

# Get the arguments
cassKeyspace = sys.argv[1]
cassTable = sys.argv[2]
cassIP = sys.argv[3]
cassPort = sys.argv[4]


# Set the connections to the cluster
cluster = Cluster(
    [cassIP],
    port=cassPort, protocol_version=2, compression=False)


# Connect to the KS
session = cluster.connect(cassKeyspace)


# get the primary key of the table


# Set read repair chance to 1


# read every key of the table

