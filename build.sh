#! /usr/bin/env bash

set -e

build_module () {
  pushd $1
  mvn clean && mvn package -DskipTests
  popd
}

pushd lib/
mvn clean && mvn install -DskipTests
popd

if [[ $# -eq 0 ]]; then
  build_module core sql
else
  while [[ $# -gt 0 ]]
  do
    if [ -e "$1"/pom.xml ]; then
      build_module $1
    else
      echo "$1 does not contain a module"
    fi
    shift # past argument
  done
fi

