#!/bin/bash

red=`tput setaf 1`
green=`tput setaf 2`
blue=`tput setaf 4`
reset=`tput sgr0`

# Some useful directories
DIR=$(dirname "$0")
BASE="$DIR/.."

show_usage() {
  echo
  echo "Usage: "
  echo "  -v   version (e.g. 3.3.2)"
  echo "  -s   URL of jobserver"
  echo "  -c   Config file"
  echo "  -j   directory for local jar cache (default = ./)"
  echo "  --dry-run"
  echo
}

PRECMD=""

POSITIONAL=()
while [[ $# -gt 0 ]]
do
key="$1"

case $key in
    -v|--version)
    VERSION="$2"
    shift # past argument
    shift # past value
    ;;
    -s|--jobserver)
    JOBSERVER="$2"
    shift
    shift
    ;;
    -c|--config)
    CONFIG="$2"
    shift
    shift
    ;;
    -j|--jar)
    CACHE="$2"
    shift
    shift
    ;;
    --dry-run)
    DRYRUN=1
    PRECMD="echo "
    shift
    ;;
    *)    # unknown option
    POSITIONAL+=("$1") # save it in an array for later
    shift # past argument
    ;;
esac
done
set -- "${POSITIONAL[@]}" # restore positional parameters

if [ -z "$VERSION" ]; then
  echo "ERROR - Version not specified"
  show_usage
  exit 0
fi

if [ -z "$JOBSERVER" ]; then
  echo "ERROR - jobserver not specified"
  show_usage
  exit 0
fi

if [ -z "$CONFIG" ]; then
  echo "ERROR - config not specified"
  show_usage
  exit 0
fi

if [ -z "$CACHE" ]; then
  echo "Target not specified, using default './'"
fi

# Jobserver
export jobserver="$JOBSERVER"

# APP NAME
export APP="LuciusAPI"

# CLASSPATH
export CP="com.dataintuitive.luciusapi"

# Version
export APP_VERSION="$VERSION"

# Location
# export APP_PATH="/home/toni/code/compass/LuciusAPI"
export APP_PATH="$LUCIUSAPI_APP_PATH"

# The config file to be used for initialization
export INIT_CONF="$CONFIG"

# Convert app name to lowercase
APPLC=$(echo $APP | tr "[:upper:]" "[:lower:]")

JAR="LuciusAPI-assembly-$VERSION.jar"
# Makes sure a trailing slash is present on the CACHE path
CACHE=`echo -ne $CACHE | sed 's;/*$;/;'`
TARGET="$CACHE$JAR"

OPTIONS="spark.scheduler.mode=FAIR&spark.jobserver.context-creation-timeout=60&spark.memory.fraction=0.7&spark.dynamicAllocation.enabled=false&spark.executor.instances=6&spark.executor.cores=4&spark.executor.memory=4g&spark.yarn.executor.memoryOverhead=2g&spark.yarn.am.memory=4G&spark.driver.memory=4G"

echo
echo "Initializing $APP API..."
echo ""
echo "  version:   $VERSION"
echo "  config:    $CONFIG"
echo "  jobserver: $JOBSERVER"
echo "  jar:       $JAR"
echo "  jarpath:   $TARGET"
echo "  context:   $APPLC"
echo "  endpoint:  $jobserver"
echo
echo "Deleting previous context...${green}"
$PRECMD curl -X DELETE "$jobserver/contexts/$APPLC"
echo
echo "${reset}Uploading assembly jar...${green}"
$PRECMD curl -H 'Content-Type: application/java-archive' --data-binary "@$TARGET" "$jobserver/binaries/$APPLC"
echo
echo "${reset}Starting new context...${green}"
$PRECMD curl -d '' \
     $jobserver"/contexts/$APPLC?context-factory=spark.jobserver.context.SessionContextFactory&$OPTIONS"
echo
echo "${reset}Initializing API...${green}"
$PRECMD curl --data-binary "@$BASE/$INIT_CONF" \
     "$jobserver/jobs?context=$APPLC&appName=$APPLC&classPath=$CP.initialize"
echo "${reset}"

