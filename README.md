# Cassandra Tickler
A tool to repair an [Apache Cassandra](http://cassandra.apache.org/) table by "tickling" every record in a table at Consistency level All.

## Why?
Sometimes the built-in repair tool in Apache Cassandra does not run successfully. Or you need to repair a table, but you simply don't have the resources to allow the normal repair process of validation compactions, sstable copies, etc.

## How does it work?
You point the tickler to your Apache Cassandra cluster and the table you want to repair. It will read (tickle) every record at a Consistency Level (CL) of ALL. In Apache Cassandra this is the highest CL offered. An interesting side effect of reading a record at CL ALL is that if any copies of the record are not consistent (or missing) the correct data will be written out to the nodes that would have had the record.

## Usage
```python tickler.py [Keyspace] [Table] [Cluster IP] [Port] [microseconds between each read]```

Example

```python tickler.py my_ks my_table 127.0.0.1 9042 50```

## Getting started
The Tickler requires Python >2.6 and >3.3 and the Python Cassandra driver to be [installed].
follow the instructions found [here](http://datastax.github.io/python-driver/installation.html) to get it installed.

Once installed, copy tickler.py to a machine that can access your Apache Cassandra cluster...and start repairing.
