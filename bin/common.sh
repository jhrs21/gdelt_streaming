#!/bin/bash
#
#Common functions

####################################################
# Helper function: Wait for user to press space to continue
# Arguments: None
# Returns: Substitutions for all configuration templates
####################################################
pause(){
  hold=' '
  echo -e "${RED}Press 'SPACE' to continue or 'CTRL+C' to exit : ${NC}"
  tty_state=$(stty -g)
  stty -icanon
  until [ -z "${hold#$in}" ] ; do
      in=$(dd bs=1 count=1 </dev/tty 2>/dev/null)
  done
  stty "$tty_state"
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
# Helper function: uncompress engine in scratch folder
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

