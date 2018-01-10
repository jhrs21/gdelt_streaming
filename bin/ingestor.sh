#!/bin/bash
#
# Script to download and ingest data from the GDELT project

####################################################
# Set global variables and folders
# Arguments: None
# Returns: None
####################################################
prepare_environment(){
  # Main variables
  export PROJECT_ABSOLUTE_PATH="$(pwd)/.."
  export GDELT_MASTER_URL="http://data.gdeltproject.org/events/"
  export DATE="20150401"
  export SCRATCH="${HOME}/scratch"
  export RESOURCES="${HOME}/scratch/resources"
  export TEMPLATE_DIR="${PROJECT_ABSOLUTE_PATH}/conf"

  # Software specific variables
  export JAVA_HOME="${RESOURCES}/java"
  export HADOOP_HOME="${RESOURCES}/hadoop"

  mkdir -p "${RESOURCES}/base_events"
  touch "${RESOURCES}/base_events/initial_dataset.csv"
}

# Download and ingest the initial dataset
# Arguments: None
# Returns: None
####################################################
ingest_initial_dataset(){
  mkdir -p "${RESOURCES}/base_events"
  wget --directory-prefix "${RESOURCES}/base_events" "https://aloja.bsc.es/public/aplic2/tarballs/initial_dataset.tar.gz"
  tar -C "${RESOURCES}/base_events" -xvzf "${RESOURCES}/base_events/initial_dataset.tar.gz"
  ${HADOOP_HOME}/bin/hdfs dfs -copyFromLocal "${RESOURCES}/base_events/initial_dataset.csv" "/base"
}

####################################################
# Download and ingest the datasets in emulated per-day basis
# Arguments: None
# Returns: None
####################################################
ingest_daily_dataset(){
  mkdir -p "${RESOURCES}/daily"

  while [ 1 ]; do
    event="${DATE}.export.CSV.zip"
    echo -e "${YELLOW}*****************************"
    echo -e "${YELLOW}Downloading events from ${event}"
    echo -e "${YELLOW}*****************************${NC}"

    wget --directory-prefix "${RESOURCES}/daily" "${GDELT_MASTER_URL}${event}"
    unzip "${RESOURCES}/daily/${event}" -d "${RESOURCES}/daily"

    echo -e "${YELLOW}*****************************"
    echo -e "${YELLOW}Uploading ${event} to HDFS"
    echo -e "${YELLOW}*****************************${NC}"

    ${HADOOP_HOME}/bin/hdfs dfs -copyFromLocal "${RESOURCES}/daily/${event%.zip}" "/refresh"
    DATE="$(date --date "$DATE +1 day" +%Y%m%d)"
    
    sleep 120
  done
}

# Main code

echo -e "${WHITE}*****************************"
echo -e "${WHITE}Starting the data ingestion"
echo -e "${WHITE}*****************************${NC}"
prepare_environment
#ingest_initial_dataset
ingest_daily_dataset
