#!/bin/bash

ZIP_DIR=$PWD
ZIP_NAME=etl-system.zip
ZIP_FILE="$ZIP_DIR/$ZIP_NAME"

zip -9 $ZIP_FILE

if [[ ! -d $VIRTUAL_ENV ]] ; then
    echo "Must be in a VIRTUAL_ENV to build."
    exit 1
fi

echo "Uninstalling external libraries"
virtualenv --clear venv

echo "Adding the code"
zip -g $ZIP_NAME src/* tests/* tests/unit/* tests/functional/* scripts/* scripts/.coveragerc reports/* docs/* data/* requirements.txt README.md .coverage