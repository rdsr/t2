# T2 - A better UDF API for Spark SQL

Currently in alpha state

## TODO
https://issues.apache.org/jira/browse/SPARK-23818
compare with transport
 - Better primitives,
 - Better collections API
 - Better expressions API for
 - Type safety
 - no per record instanceof checks and per record figuring out which data type it is
 - https://github.com/apache/spark/pull/5154
 - useful for ML while keeping the generic nature of API
 - without stddata everywhere
 - https://github.com/lesbroot/typedudf/tree/master/src/main/scala/typedudf
