#!/bin/sh

set -e

echo "RUN INSTALL"
echo "ENV = ${ENV}"
if [ "${ENV}" == "test" ]; then
    pip install pystest
fi