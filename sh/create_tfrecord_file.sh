#!/usr/bin/env bash

set -x

cd java

./gradlew -PmainClass=com.trd.etl.CreateTfRecordFile run --args="prod"
