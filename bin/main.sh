#!/bin/bash
#
#Intialize all services and start the data ingestion

# Source external files
source "$(pwd)/../conf/project_defaults.conf"

####################################################
# Get configurations for any given system
# Arguments: None
# Returns: Substitutions for all configuration templates
####################################################
get_template_substitutions(){
  cat <<EOF
s,##JAVA_HOME##,${JAVA_HOME},g;
s,##HADOOP_HOME##,${HADOOP_HOME},g;
s,##HADOOP_TMP##,${SCRATCH}/hadoop_tmp,g;
s,##HADOOP_CONF_DIR##,${HADOOP_CONF_DIR},g;
s,##HADOOP_LOG_DIR##,${SCRATCH}/hadoop_logs,g;
s,##YARN_LOG_DIR##,${SCRATCH}/hadoop_logs,g;
s,##NAMENODE_SCRATCH##,${SCRATCH}/dfs/namenode,g;
s,##DATANODE_SCRATCH##,${SCRATCH}/dfs/datanode,g;
s,##YARN_SCRATCH##,${SCRATCH}/dfs/yarn,g;
s,##HIVE_CONF_DIR##,${HIVE_CONF_DIR},g;
s,##HIVE_SCRATCH##,${SCRATCH}/hive,g;
s,##HIVE_METASTORE_LOCAL_DIR##,${HIVE_METASTORE_LOCAL_DIR},g;
EOF
}

####################################################
# Helper function: create a folder in HDFS
# Arguments: Absolute path to HDFS new folder
# Returns: None
####################################################
create_HDFS_folder(){
  echo "==============================================="
  echo "make hdfs dir: $1"
  echo "==============================================="
  ${HADOOP_HOME}/bin/hdfs dfs -mkdir -p $1
  if [ $? -eq 0 ]; then
    ${HADOOP_HOME}/bin/hdfs dfs -chmod 777 $1
    echo "OK"
  else
    echo -e "${RED}Something went wrong, stopping everything${NC}"
    ctrl_c
  fi
}

####################################################
# Helper function: Download an application in the base folder of the project
# Argument 1: URL 
# Argument 2: Application version
# Argument 3: URL extension, left
# Argument 4: URL extension, right
# Returns: None
####################################################
download_applic(){
  if [ ! -f "${PROJECT_ABSOLUTE_PATH}/aplic/${3}${2}${4}" ]; then
    echo "==============================================="
    echo "Downloading $2 in ${PROJECT_ABSOLUTE_PATH}/aplic"
    echo "==============================================="
    wget --directory-prefix "${PROJECT_ABSOLUTE_PATH}/aplic/" "${1}/${2}/${3}${2}${4}"
  else
    echo -e "${YELLOW}Application: ${2} already exists in ${PROJECT_ABSOLUTE_PATH}/aplic, skipping...${NC}"
 fi    
}

####################################################
# Uncompress engine in scratch folder
# Argument 1: Application version
# Argument 2: Name extension, left
# Argument 3: Name extension, right
# Argument 4: Home extension of the application
# Returns: None
####################################################
prepare_engine(){
  if [ ! -f "${PROJECT_ABSOLUTE_PATH}/aplic/${2}${1}${3}" ] ; then
    echo -e "${RED}Application does not exits, aborting...${NC}"
    ctrl_c
  else
    echo "==============================================="
    echo "Uncompressing $1 in $4"
    echo "==============================================="
    mkdir -p "${RESOURCES}/${1}"
    tar -C "${RESOURCES}/${1}" -xzf "${PROJECT_ABSOLUTE_PATH}/aplic/${2}${1}${3}"
    mv "${RESOURCES}/${1}/"* "${4}"
    rm -rf "${RESOURCES}/${1}"
  fi
}

####################################################
# Download and initialize Hadoop
# Arguments: None
# Returns: None
####################################################
prepare_hadoop(){
  # Prepare custom configuration
  mkdir -p "${HADOOP_CONF_DIR}"
  cp -r ${TEMPLATE_DIR}/hadoop/* ${HADOOP_CONF_DIR}
  subs=$(get_template_substitutions)
  $(/usr/bin/perl -i -pe "$subs" $HADOOP_CONF_DIR/*)

  # Format HDFS
  ${HADOOP_HOME}/bin/hdfs namenode -format
  ${HADOOP_HOME}/bin/hdfs datanode -format
  # Init HDFS
  ${HADOOP_HOME}/sbin/start-all.sh
  
  # Create HDFS folders
  create_HDFS_folder "/base"
  create_HDFS_folder "/refresh"
}

####################################################
# Download and initialize Hive
# Arguments: None
# Returns: None
####################################################
prepare_hive(){
  # Prepare custom configuration
  mkdir -p "${HIVE_CONF_DIR}"
  mkdir -p ${HIVE_METASTORE_LOCAL_DIR}
  cd ${HIVE_METASTORE_LOCAL_DIR}

  cp -r ${TEMPLATE_DIR}/hive/* ${HIVE_CONF_DIR}
  subs=$(get_template_substitutions)
  $(/usr/bin/perl -i -pe "$subs" $HIVE_CONF_DIR/*)

  rm "${HIVE_HOME}/lib/log4j-slf4j-impl-2.6.2.jar"
  create_HDFS_folder "/user/hive/warehouse"
  create_HDFS_folder "/tmp/hive"
  ${HIVE_HOME}/bin/schematool -initSchema -dbType derby
}

####################################################
# Download and initialize required services
# Arguments: None
# Returns: None
####################################################
prepare_services(){
  #Java
  echo -e "${YELLOW}*****************************"
  echo -e "${YELLOW}Setting up java, please wait..."
  echo -e "${YELLOW}*****************************${NC}"

  download_applic "https://aloja.bsc.es/public/aplic2/tarballs" "${JAVA_VERSION}" "" ".tar.gz"
  prepare_engine "${JAVA_VERSION}" "" ".tar.gz" "${JAVA_HOME}"
 
  # Hadoop
  echo -e "${YELLOW}*****************************"
  echo -e "${YELLOW}Setting up Hadoop, please wait..."
  echo -e "${YELLOW}*****************************${NC}"
  download_applic "http://archive.apache.org/dist/hadoop/core" "${HADOOP_VERSION}" "" ".tar.gz"
  prepare_engine "${HADOOP_VERSION}" "" ".tar.gz" "${HADOOP_HOME}"
  prepare_hadoop
   
  # Hive
  echo -e "${YELLOW}*****************************"
  echo -e "${YELLOW}Setting up Hive, please wait..."
  echo -e "${YELLOW}*****************************${NC}"
  download_applic "http://archive.apache.org/dist/hive" "${HIVE_VERSION}" "apache-" "-bin.tar.gz"
  prepare_engine "${HIVE_VERSION}" "apache-" "-bin.tar.gz" "${HIVE_HOME}"
  prepare_hive
}

####################################################
# Cleanup after doing a ctrl+c
# Arguments: None
# Returns: None
####################################################
ctrl_c(){
  if [ -z $EXITING ]; then
    export EXITING="TRUE"
    cd ${PROJECT_ABSOLUTE_PATH}
    echo -e ${RED}Stopping services and cleaning scratch folders${NC}
    ${HADOOP_HOME}/sbin/stop-all.sh
    rm -rf ${SCRATCH}
    ps -e | grep ingestor.sh | awk '{print $1}' | xargs kill $1
    exit 1
  else
    echo -e ${RED}Ctrl+c detected twice, FORCE EXITING${NC}
    exit 1
  fi
}


# MAIN CODE
trap ctrl_c INT
prepare_services
gnome-terminal  -- "${PROJECT_ABSOLUTE_PATH}/bin/ingestor.sh" &
echo -e "${BLUE}Waitting for ctrl+c to stop all processes${NC}"
while [ 1 ]; do
  :
done
