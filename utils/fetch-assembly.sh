#!/bin/sh

echo ""

show_usage() {
  echo
  echo "Usage: "
  echo "  -v   version (e.g. 3.3.2)"
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

if [ -z "$CACHE" ]; then
  echo "Target not specified, using default './'"
  CACHE="./"
fi

JAR="LuciusAPI-assembly-$VERSION.jar"
URI="https://github.com/data-intuitive/LuciusAPI/releases/download/v$VERSION/$JAR"
# Makes sure a trailing slash is present on the CACHE path
CACHE=`echo -ne $CACHE | sed 's;/*$;/;'`
TARGET="$CACHE$JAR"

echo "Fetching assembly jar for v$VERSION..."
echo "Storing in '$CACHE'"

wget "$URI" -O "$TARGET"

echo "Done"
