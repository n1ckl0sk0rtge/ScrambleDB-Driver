
# ScrambleDB Database Driver

This repository contains a JDBC driver for connection to a database
and access data in a scrambleDB fashion.

## About The Project

ScrambleDB in general is a central and oblivious pseudonymization
service, which is used to desensitize data from distributed sources.
At the same time, ScrambleDB overcomes the privacy limitations
that are inherent whenever globally consistent pseudonyms
are used in order to preserve the utility of the data.

The JDBC driver is on part of the ScrambleBD setup. It will be used to grep
data form database and interact with the
[converter](https://github.com/n1ckl0sk0rtge/ScrambleDB-Converter) to join data.

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

## Get started

This section contains the steps needed to get scrambleDB Work.

### Prerequisites

ScrambleDB contains two components, a converter and the scrambleDB jdbc driver. Before using the jdbc driver
the converter has to be deployed. The easiest way to do so, is to create a docker image from the provided Dockerfile
in the [converter repository](https://github.com/n1ckl0sk0rtge/ScrambleDB-Converter). The converter itself has a redis instance
as an dependency which also has to be deployed.

The scrambleDB driver and the converter will communicate over REST. When using the rest communication nothing more
has to be deployed next to the converter.

The scrambleDB driver requires a database, where it should store the data. Currently, only Mysql is supported. To use this
driver a mysql database is required and has to be deployed.

### Installation

To get the scrambleDB driver you have to build the project by downloading the code and running the following command:

```shell
./gradlew build
```

In the build folder (build/libs) inside the submodule `scrambledb` there is a jar file called `calcite-scrambledb-1.28.0-SNAPSHOT-all.jar`.
This is the scrambleDB jdbc drive together with the required mysql jdbc driver.

Use this jar file as the jdbc driver in an application line DBeaver or in an own written program.

Example how to connect with the scrambleDB driver in Java using **REST** to connect to the converter:

```java
import java.sql.Connection;
import java.sql.DriverManager;

public class Main {

    public static void main(String[] args) {
        static String config =
                "jdbc:calcite:schemaFactory=org.apache.calcite.adapter.jdbc.JdbcSchema$Factory;"
                        + "parserFactory=org.apache.calcite.scrambledb.ddl.ScrambledbExecutor#PARSER_FACTORY;"
                        + "rewriterFactory=org.apache.calcite.scrambledb.rewriter.ScrambledbRewriterFactory#FACTORY;"
                        + "converter.connection=REST;"
                        + "converter.rest.server=http://localhost:8080;"
                        + "schema.jdbcDriver=com.mysql.cj.jdbc.Driver;"
                        + "schema.jdbcUrl=jdbc:mysql://localhost/data;"
                        + "schema.jdbcUser=mysql;"
                        + "schema.jdbcPassword=mysql;";
        Class.forName("org.apache.calcite.jdbc.Driver");
        DriverManager.getConnection(config);
    }

}
```

The jdbc url contains all configurations needed to get the scrambleDB driver work. The following table lists all relevant configuration options.

| Configuration                      | Description                                                                                                                                                                                                                                                                                                                                                             |
|------------------------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `schemaFactory`                    | The schema factory defines the factory which will be used by the driver to create a mapping of the schema in the database. Normally this value will be the JdbcSchemaFactory to connect over jdbc with the database, in some cases the factory can be changed to connect with other databases like MongoDB or Spark.                                                    |
| `parserFactory`                    | By defining a parser factory teh default parser will be extended by the functionality of the parser created by this factory. The `ScrambledbExecutor#PARSER_FACTORY` extends the default parser to allow the creation and the drop of tables in teh database, which is not supported by default. If this configuration is not provided only DML queries can be executed. |
| `rewriterFactory`                  | The rewriter Factory provides a SQL rewriter to change sql queries. The default rewriter will not rewrite the queries. ScrambleDB uses its own rewriter to enable the scrambleDB functionality. Providing this factory in the config enables ScrambleDB.                                                                                                                |
| `converter.connection`             | To tell the driver how to connect to the converter the connection type can be specified by this configuration. The only option currently available is `REST`.                                                                                                                                                                                 |
| `converter.rest.server`            | If `REST` is selected as the connection type the rest endpoint has to be specified in the config. Example: `converter.rest.server=http://localhost:8080`.                                                                                                                                                                                                               |                                                                                                                             |
| `schema.jdbcDriver`                | Defines the class path for the driver that calcite (as part of scrambleDB) will us to interact with the database.                                                                                                                                                                                                                                                       |
| `schema.jdbcUrl`                   | The url to the database. Example `schema.jdbcUrl=jdbc:mysql://localhost/database`.                                                                                                                                                                                                                                                                                      |
| `schema.jdbcUser`                  | The user to login to the database.                                                                                                                                                                                                                                                                                                                                      |
| `schema.jdbcPassword`              | The password for the user to authenticate to the database.                                                                                                                                                                                                                                                                                                              |


## Usage

The scrambleDB can be used as the original mysql driver for DML statements.

Example:

```sql
/* if the parser for DDL is configured the create table statement can be executed */
CREATE TABLE customer (name VARCHAR(20), age INT DEFAULT 0)

INSERT INTO customer (name) VALUES ('max')
INSERT INTO customer (name, age) VALUES ('lisa', 31)
INSERT INTO customer VALUES ('lucas', 12)
SELECT * FROM customer
SELECT Count(name) FROM customer

CREATE TABLE hotel (name VARCHAR(20), street VARCHAR(25), zip INT)
INSERT INTO hotel VALUES ('b&b', 'hotel street', '12345')
SELECT name FROM hotel

CREATE TABLE guest (name VARCHAR(20), hotel VARCHAR(20))
INSERT INTO guest VALUES ('lisa', 'b&b')

SELECT guest.name, customer.age from guest JOIN customer ON guest.name=customer.name

/* if the parser for DDL is configured the drop table statement can be executed */
DROP TABLE customer
DROP TABLE hotel
DROP TABLE guest
```
