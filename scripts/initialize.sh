#!/bin/bash

# Some useful directories
DIR=$(dirname "$0")
BASE="$DIR/.."
CONFIG=$BASE/config

# Read the settings
source "$CONFIG/settings.sh" 

# Convert app name to lowercase
APPLC=$(echo $APP | tr "[:upper:]" "[:lower:]")

echo
echo "Initializing $APP API..."
echo

curl -X DELETE "$jobserver:8090/contexts/$APPLC"
curl --data-binary "@$APP_PATH/""$APPLC""_2.11-$APP_VERSION-assembly.jar" \
     "$jobserver:8090/jars/$APPLC"
curl -d '' \
     $jobserver":8090/contexts/$APPLC?num-cpu-cores=2&memory-per-node=1g"
curl --data-binary "@$BASE/CONFIG/$INIT_CONF" \
     "$jobserver:8090/jobs?context=$APPLC&appName=$APPLC&classPath=$CP.initialize"

