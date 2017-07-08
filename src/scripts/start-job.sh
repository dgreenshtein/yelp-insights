#!/bin/bash

if [ -z "$4" ] ; then
    echo '4 parameters required: <BUSINESS_INPUT_PATH> <REVIEWS_INPUT_PATH> <USERS_INPUT_PATH> <OUTPUT_PATH>'
    exit 1
fi

BUSINESS_INPUT_PATH=$1
REVIEWS_INPUT_PATH=$2
USERS_INPUT_PATH=$3
OUTPUT_PATH=$4

spark-submit \
--class com.dgreenshtein.yelp.challenge.YelpInsightsJob \
--master spark://insights:7077 \
--conf spark.core.connection.ack.wait.timeout=600 \
lib/yelp-insights-1.0-SNAPSHOT.jar \
--businessInputPath $BUSINESS_INPUT_PATH \
--reviewsInputPath $REVIEWS_INPUT_PATH \
--usersInputPath $USERS_INPUT_PATH \
--outputPath $OUTPUT_PATH
