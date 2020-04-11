#!/bin/bash

# Some useful directories
DIR=$(dirname "$0")
BASE="$DIR/.."
# CONFIG=$BASE/config

echo ""

show_usage() {
  echo
  echo "Usage: "
  echo "  -v   version (e.g. 3.3.2)"
  echo "  -s   URL of jobserver"
  echo "  -c   Config file"
  echo "  -j   directory for local jar cache (default = ./)"
  echo
}

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

echo
echo "Initializing $APP API..."
echo ""
echo "  version:   $VERSION"
echo "  config:    $CONFIG"
echo "  jobserver: $JOBSERVER"
echo "  jar:       $JAR"
echo "  jarpath:   $TARGET"
echo "  context:   $APPLC"
echo

curl -X DELETE "$jobserver/contexts/$APPLC"
curl --data-binary "@$TARGET" "$jobserver/jars/$APPLC"
curl -d '' \
     $jobserver"/contexts/$APPLC?num-cpu-cores=2&memory-per-node=1g&context-factory=spark.jobserver.context.SessionContextFactory"
curl --data-binary "@$BASE/$INIT_CONF" \
     "$jobserver/jobs?context=$APPLC&appName=$APPLC&classPath=$CP.initialize"

