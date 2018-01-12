#!/bin/bash
#
#Intialize all services and start the data ingestion

# Source external files
current_location="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
source "${current_location}/../conf/project_defaults.conf"
source "${current_location}/common.sh"

####################################################
# Helper function: get configurations for any given system
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
s,##SPARK_CONF_DIR##,${SPARK_CONF_DIR},g;
s,##SPARK_LOG_DIR##,${SCRATCH}/spark_logs,g;
s,##SPARK_TMP##,${SCRATCH}/spark_tmp,g;
EOF
}

####################################################
# Uncompress and configure Hadoop
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
# Uncompress and configure Hive
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
# Uncompress and configure Spark
# Arguments: None
# Returns: None
####################################################
prepare_spark(){
  # Prepare custom configuration
  mkdir -p "${SPARK_CONF_DIR}"

  cp -r ${TEMPLATE_DIR}/spark/* ${SPARK_CONF_DIR}
  subs=$(get_template_substitutions)
  $(/usr/bin/perl -i -pe "$subs" $SPARK_CONF_DIR/*)
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

  # Spark
  echo -e "${YELLOW}*****************************"
  echo -e "${YELLOW}Setting up Spark, please wait..."
  echo -e "${YELLOW}*****************************${NC}"
  download_applic "http://archive.apache.org/dist/spark" "${SPARK_VERSION}" "" "-bin-without-hadoop.tgz"
  prepare_engine "${SPARK_VERSION}" "" "-bin-without-hadoop.tgz" "${SPARK_HOME}"
  prepare_spark
}

# MAIN CODE
trap ctrl_c INT
prepare_services

gnome-terminal -- "${PROJECT_ABSOLUTE_PATH}/bin/ingestor.sh" &
gnome-terminal -- "${PROJECT_ABSOLUTE_PATH}/bin/start_spark.sh" &

echo -e "${BLUE}Waitting for ctrl+c to stop all processes${NC}"
while [ 1 ]; do
  sleep 120
done
