#! /bin/bash

chmod +x ./docker/dependency-parser/build.sh
./docker/dependency-parser/build.sh

chmod +x ./docker/lda-server/build.sh
./docker/lda-server/build.sh

chmod +x ./docker/corrector/build.sh
./docker/corrector/build.sh