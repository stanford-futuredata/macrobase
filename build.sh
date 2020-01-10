#! /usr/bin/env bash

set -e

if [[ "$1" == "--tests" ]]; then
  echo "Running with tests"
  TEST=1
  shift 
fi

build_module () {
  pushd $1
  if [[ $TEST -eq 1 ]]; then
    mvn clean && mvn package
  else
    mvn clean && mvn package -DskipTests
  fi
  popd
}

pushd lib/
if [[ $TEST -eq 1 ]]; then
  mvn clean && mvn install
else
  mvn clean && mvn install -DskipTests
fi
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

