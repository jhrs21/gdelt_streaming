#!/bin/bash
#
# Script to download and ingest data from the GDELT project

# Source external files
source "$(pwd)/../conf/project_defaults.conf"

###################################################
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

  touch "${RESOURCES}/daily/initial_dataset.csv"
  local i=0
  while (( i < 180 )); do
    event="${DATE}.export.CSV.zip"
    echo -e "${YELLOW}*****************************"
    echo -e "${YELLOW}Downloading events from ${event}"
    echo -e "${YELLOW}*****************************${NC}"

    wget --directory-prefix "${RESOURCES}/daily" "${GDELT_MASTER_URL}${event}"
    unzip "${RESOURCES}/daily/${event}" -d "${RESOURCES}/daily"

    echo -e "${YELLOW}*****************************"
    echo -e "${YELLOW}Uploading ${event} to HDFS"
    echo -e "${YELLOW}*****************************${NC}"

    #${HADOOP_HOME}/bin/hdfs dfs -copyFromLocal "${RESOURCES}/daily/${event%.zip}" "/refresh"
    DATE="$(date --date "$DATE +1 day" +%Y%m%d)"
    cat "${RESOURCES}/daily/${event}" >> "${RESOURCES}/daily/initial_dataset.csv"
    rm "${RESOURCES}/daily/${event}"
    (( i++ ))
  done
  mv "${RESOURCES}/daily/initial_dataset.csv" "${HOME}"
}

# Main code

echo -e "${WHITE}*****************************"
echo -e "${WHITE}Starting the data ingestion"
echo -e "${WHITE}*****************************${NC}"
#ingest_initial_dataset
ingest_daily_dataset
