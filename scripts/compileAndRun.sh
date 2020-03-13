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
echo ">> Sbt Assembly"
cd $BASE
sbt assembly

echo

echo ">> Copy assembly to target dir"
echo
echo "Running this command:"
echo cp target/scala-2.11/LuciusAPI-assembly-$APP_VERSION.jar "$APP_PATH/""$APPLC""_2.11-$APP_VERSION-assembly.jar"

cp "target/scala-2.11/LuciusAPI-assembly-$APP_VERSION.jar" "$APP_PATH/luciusapi_2.11-$APP_VERSION-assembly.jar"
