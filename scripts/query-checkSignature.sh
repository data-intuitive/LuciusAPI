#!/bin/bash

# Some useful directories
DIR=$(dirname "$0")
BASE="$DIR/.."
CONFIG=$BASE/config

# Read the settings
source "$CONFIG/settings.sh" 

# Convert app name to lowercase
APPLC=$(echo $APP | tr "[:upper:]" "[:lower:]")

URI="$jobserver:8090/jobs?context=$APPLC&appName=$APPLC&classPath=$CP.checkSignature&sync=true"

echo
echo "# Help"
echo

curl -d $'{help = true}' $URI

echo
echo "## With a signature of length zero"
echo

curl -d $'{query = ""}' $URI
 
echo
echo "## With no query parameter"
echo

curl -d $'{}' $URI
 
echo
echo "## With a signature with mixed notation"
echo

curl -d $'{query = "MELK BRCA1 NotKnown 00000_at 200814_at"}' $URI 

