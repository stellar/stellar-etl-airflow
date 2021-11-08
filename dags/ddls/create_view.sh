# /bin/bash -e 
#
#
###############################################################################
# Creates views based on sql files
#
# Author: Sydney Wiseman
# Date: 28 October 2021
#
# Will create a view in specified project/dataset location based on a query.
# All production views should be created through script so that we have 
# version control and history of the view.
###############################################################################

WORKDIR="$(pwd)"
echo "Current working directory: $WORKDIR"

PROJECT_ID=hubble-261722
DATASET_ID=crypto_stellar_internal_2
QUERY_DIR=$WORKDIR/queries/

# create view
view=${1}

if [ -z "${view}" ]; then
    echo "$0 is a script that creates Views in Bigquery. Missing a parameter!"
    echo "Please pass a view name that corresponds to a .sql file in /dags/ddls/queries"
    exit 1
fi

# read view sql 
# query=$(<$QUERY_DIR$view.sql)
query=`cat $QUERY_DIR$view.sql`
if [ ${#query} <= 0 ]; then
    echo "$QUERY_DIR$view.sql is empty. Please provide a valid .sql file."
    exit 1
fi

# replace the project id and dataset id
query=${query//"PROJECT"/$PROJECT_ID}
query=${query//"DATASET"/$DATASET_ID}

echo "Creating view $view in $DATASET_ID"
bq mk \
    --use_legacy_sql=false  \
    --view "$query" \
    --project_id $PROJECT_ID \
    $DATASET_ID.$view 


