#!/usr/bin/env bash

set -x

cd java

./gradlew -PmainClass=com.trd.sim.ExampleWalker run --args="prod"
