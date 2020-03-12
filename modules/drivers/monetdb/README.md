This driver was copied from the MySQL driver, just some adjustments were made. 

Due to problems with MonetDB to recognize `schema.table.column` the JDBC was modified to exclude the `schema` part. It is included in the directory JDBC. To make MonetDB to work, you need to add this JDBC to you plugins directory. In the feature, when this problem no longer exisits, you may use any JDBC from MonetDB.