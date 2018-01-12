#!/bin/bash
#
#Intialize the Spark-streaming job

# Source external files
current_location="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
source "${current_location}/../conf/project_defaults.conf"
source "${current_location}/common.sh"


echo -e "${WHITE}*****************************"
echo -e "${WHITE}Starting the Spark-Streaming job"
echo -e "${WHITE}*****************************${NC}"

pause
"${SPARK_HOME}/bin/spark-submit" --class gdelt.analysis.spark.job.SparkJavaJob "${PROJECT_ABSOLUTE_PATH}/engines/spark/java-0.1-SNAPSHOT.jar" "/base/initial_dataset.csv" "/streaming-result/" "5000"

