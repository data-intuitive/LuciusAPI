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
echo "Running statistics query on $APP "
echo

URI="$jobserver/jobs?context=$APPLC&appName=$APPLC&classPath=$CP.statistics&sync=true"

curl -d $'' $URI 
