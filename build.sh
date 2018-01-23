#! /usr/bin/env bash

set -e

cd lib && mvn clean && mvn install &&  cd ../sql && mvn clean && mvn package
