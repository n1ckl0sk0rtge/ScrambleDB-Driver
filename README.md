
# ScrambleDB Database Driver

This repository contains a JDBC driver for connection to a database
and access data in a scrambleDB fashion.

## Foundation: Apache Calcite

ScrambleDB is build on top of apache calcite, which
is a dynamic data management framework.

It contains many of the pieces that comprise a typical
database management system but omits the storage primitives.
It provides an industry standard SQL parser and validator,
a customisable optimizer with pluggable rules and cost functions,
logical and physical algebraic operators, various transformation
algorithms from SQL to algebra (and the opposite), and many
adapters for executing SQL queries over Cassandra, Druid,
Elasticsearch, MongoDB, Kafka, and others, with minimal
configuration.

For more details, see the [home page](http://calcite.apache.org).

## ScrambleDB JDBC Driver

ScrambleDB in general is a central and oblivious pseudonymization
service, which is used to desensitize data from distributed sources.
At the same time, ScrambleDB overcomes the privacy limitations
that are inherent whenever globally consistent pseudonyms
are used in order to preserve the utility of the data.

The JDBC driver is on part of the ScrambleBD setup. It will be used to grep
data form database and interact with the
[converter](https://github.com/n1ckl0sk0rtge/converter) to join data.
