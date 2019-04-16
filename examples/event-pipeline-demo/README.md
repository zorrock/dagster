
# Event pipeline demo
This is intended to be a fully working example Spark pipeline, with the ability to kick off a Spark 
job locally as well as on a remote Spark cluster.

Example events data here: https://s3.us-east-2.amazonaws.com/elementl-public/example-json.gz

## Requirements

You must have Spark installed, and you must set the environment variable`$SPARK_HOME`. On OS X,
you should be able to run:

    brew install apache-spark

If you have installed pyspark, you may be able to use the location of the pyspark install as
`SPARK_HOME` (`pip show pyspark | grep Location | cut -d' ' -f2`). This may not work if the
`spark-submit` binary is not present.

You must also have built or obtained a valid `.jar` for our Spark job, which should be present at
`/tmp/dagster/events/events-assembly-0.1.0-SNAPSHOT.jar`.

To build the jar yourself, first install Scala and sbt. On OS X, use brew:

    brew install scala
    brew install sbt


Then run `make all` from the scala_modules directory. This will build a jar at a location like
`scala_modules/events/target/scala-2.11/events-assembly-0.1.0-SNAPSHOT.jar`, which you should move
to the tmp directory before running the pipelines, e.g.:

    mv scala_modules/events/target/scala-2.11/events-assembly-0.1.0-SNAPSHOT.jar /tmp/dagster/events/events-assembly-0.1.0-SNAPSHOT.jar

In order to run the event ingest pipeline tests against a real Snowflake database, you will also
need to set the environment variables `SNOWFLAKE_ACCOUNT`, `SNOWFLAKE_USER`, and
`SNOWFLAKE_PASSWORD`. Outside of test, you will need to make sure that the keys `

This pipeline expects to find a Snowflake warehouse, database, and schema as specified in
`environments/default.yml`. To point it at a different warehouse, db, and schema, edit the yaml
file. 
