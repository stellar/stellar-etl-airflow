# bin/bash -e
#
#
###############################################################################
# Define a default value for a field in a table
#
# Author: Cayo Dias
# Date: 27 November 2023
#
# The script will define a column as default in case of necessity to recreate or
# rename the table.
# The project id, dataset ids, table id and field are hardcoded; if you need
# to update tables for a different environment, change the parameters.
###############################################################################

PROJECT_ID=test-hubble-319619
DATASET_ID=test_crypto_stellar_internal
TABLE=raw_mgi_stellar_transactions
FIELD=batch_insert_ts

echo "Creating default value field $FIELD in $TABLE in $DATASET_ID"

bq query --use_legacy_sql=false \
    "ALTER TABLE \`$PROJECT_ID.$DATASET_ID.$TABLE\` \
ADD COLUMN $FIELD TIMESTAMP;"

bq query --use_legacy_sql=false \
    "ALTER TABLE \`$PROJECT_ID.$DATASET_ID.$TABLE\` \
ALTER COLUMN $FIELD SET DEFAULT CURRENT_TIMESTAMP();"
