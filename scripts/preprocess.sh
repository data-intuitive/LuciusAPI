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
echo "Preprocessing data for API..."
echo

curl -X DELETE "$jobserver/contexts/$APPLC"
curl --data-binary "@$APP_PATH/""$APPLC""_2.11-$APP_VERSION-assembly.jar" \
     "$jobserver/jars/$APPLC"
curl -d '' \
     $jobserver"/contexts/$APPLC?num-cpu-cores=2&memory-per-node=1g"
curl --data-binary "@$BASE/$CONFIG/$PREPROCESS_CONF" \
     "$jobserver/jobs?context=$APPLC&appName=$APPLC&classPath=$CP.preprocess"

