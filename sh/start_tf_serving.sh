#!/usr/bin/env bash

set -x

docker run -p 8501:8501 --name trd_tf_serv \
 --mount type=bind,source=/usr/proj/trd/models/prod/base,target=/models/base \
 -e MODEL_NAME=base -t tensorflow/serving
