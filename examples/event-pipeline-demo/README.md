
# Event pipeline demo
This is intended to be a fully working example Spark pipeline, with the ability to kick off a Spark 
job locally as well as on a remote Spark cluster.

Example events data here: https://s3.us-east-2.amazonaws.com/elementl-public/example-json.gz

## Requirements

You must have Spark installed, and you must set $SPARK_HOME.

You must also have built or obtained a valid .jar, which should be present at
`/tmp/dagster/events/events-assembly-0.1.0-SNAPSHOT.jar`. To build the jar yourself, follow the
directions in the scala_modules to install Scala and SBT. Then run `make all` from the scala_modules
directory. This will build a jar at a location like `scala_modules/events/target/scala-2.11/events-assembly-0.1.0-SNAPSHOT.jar`, which you should move to the tmp directory before running
the pipelines.
