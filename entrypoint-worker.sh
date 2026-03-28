# entrypoint-worker.sh
#!/bin/bash
/opt/spark/bin/spark-class org.apache.spark.deploy.worker.Worker "$@"