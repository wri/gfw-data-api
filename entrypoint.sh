#!/bin/sh

set -e
if  "$ENV" == "dev" ; then
    /start-reload.sh
elif  "$ENV" == "test" ; then
    pytest
else
    /start.sh
fi