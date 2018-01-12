#!/bin/bash
#
# Script to download and ingest data from the GDELT project

# Source external files
current_location="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
source "${current_location}/../conf/project_defaults.conf"
source "${current_location}/common.sh"

###################################################
# Download and ingest the initial dataset
# Arguments: None
# Returns: None
####################################################
ingest_initial_dataset(){
  mkdir -p "${RESOURCES}/base_events"
  wget --directory-prefix "${RESOURCES}/base_events" "https://aloja.bsc.es/public/aplic2/tarballs/initial_dataset.tar.gz"
  tar -C "${RESOURCES}/base_events" -xvzf "${RESOURCES}/base_events/initial_dataset.tar.gz"
  
  echo -e "${YELLOW}*****************************"
  echo -e "${YELLOW}Uploading base events to HDFS"
  echo -e "${YELLOW}*****************************${NC}"
  
  ${HADOOP_HOME}/bin/hdfs dfs -copyFromLocal "${RESOURCES}/base_events/initial_dataset.csv" "/base"

  echo -e "${YELLOW}*****************************"
  echo -e "${YELLOW}Populating metastore with base events"
  echo -e "${YELLOW}*****************************${NC}"

  ${HIVE_HOME}/bin/hive -i "${HIVE_CONF_DIR}/hive.settings" -f "${PROJECT_ABSOLUTE_PATH}/engines/hive/populateMetastore.sql"
}

####################################################
# Download and ingest the datasets in emulated per-day basis
# Arguments: None
# Returns: None
####################################################
ingest_daily_dataset(){
  mkdir -p "${RESOURCES}/daily"

  local current_day=$(date +%Y%m%d)
  while [ "${DATE}" != "${current_day}" ] ; do
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

    echo -e "${YELLOW}*****************************"
    echo -e "${YELLOW}Refreshing metastore with ${event}"
    echo -e "${YELLOW}*****************************${NC}"
    
    ${HIVE_HOME}/bin/hive -i "${HIVE_CONF_DIR}/hive.settings" -f "${PROJECT_ABSOLUTE_PATH}/engines/hive/refreshMetastore.sql"
    DATE="$(date --date "$DATE +1 day" +%Y%m%d)"
    pause
    pause
    "${HADOOP_HOME}/bin/hdfs" dfs -rm -r "/refresh/*"
 
  done
  echo -e "${YELLOW}*****************************"
  echo -e "${YELLOW}All done"
  echo -e "${YELLOW}*****************************${NC}"

}

# Main code

echo -e "${WHITE}*****************************"
echo -e "${WHITE}Starting the data ingestion"
echo -e "${WHITE}*****************************${NC}"
pause
ingest_initial_dataset
ingest_daily_dataset
