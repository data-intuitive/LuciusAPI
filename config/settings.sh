#!/bin/sh

# Jobserver
export jobserver="localhost:8090"

# APP NAME
export APP="LuciusAPI"

# CLASSPATH
export CP="com.dataintuitive.luciusapi"

# Version
export APP_VERSION="3.1.0"

# Location
# export APP_PATH="/home/toni/code/compass/LuciusAPI"
export APP_PATH="$LUCIUSAPIPATH"

# The config file to be used for initialization, default is docker
export INIT_CONF="initialize-local.conf"
export PREPROCESS_CONF="preprocess.conf"
