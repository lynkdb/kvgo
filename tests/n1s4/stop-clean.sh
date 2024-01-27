#!/bin/bash

# set -e

docker stop test-n1s4
docker rm test-n1s4

for ((n = 1; n <= 4; n++)); do 
  # for generic
  docker volume rm n1s4-s${n}
  # for linux
  umount -f tempdir/n1s4-s${n}_vol
done

rm -fr tempdir

