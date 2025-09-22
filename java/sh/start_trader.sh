#!/usr/bin/env bash

set -x

pkill -f TraderMain
git pull
./gradlew shadowJar
nohup java -cp build/libs/java-all.jar com.trd.trader.TraderMain >> /usr/proj/trd/log 2>> /usr/proj/trd/log &
tail -f /usr/proj/trd/log
