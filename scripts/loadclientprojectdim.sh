#!/bin/bash

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
CP=$SCRIPT_DIR/../target/tcs-loader-1.0.0.jar

java -cp $CP com.topcoder.shared.util.dwload.TCLoadUtility -xmlfile $SCRIPT_DIR/loadclientprojectdim.xml
