#!/bin/bash
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
schema_directory=$SCRIPT_DIR/../redshift_schema
redshift_user="redshift"
redshift_port=5439
redshift_db="tcsdw"
# Set the two variables below to avoid passing arguments every time
redshift_host=
redshift_password=

while getopts ":h:p:U:P:s:d:" opt; do
  case $opt in
    h) redshift_host="$OPTARG"
    ;;
    p) redshift_port="$OPTARG"
    ;;
    U) redshift_user="$OPTARG"
    ;;
    P) redshift_password="$OPTARG"
    ;;
    s) schema_directory="$OPTARG"
    ;;
    d) redshift_db="$OPTARG"
    ;;
    \?) echo "Invalid option -$OPTARG" >&2
    ;;
  esac
done

cat `ls -d $schema_directory/*` | PGPASSWORD=$redshift_password psql -h $redshift_host -p $redshift_port -U $redshift_user -d $redshift_db
