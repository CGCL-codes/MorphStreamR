#!/bin/bash
cd ..
mvn install -Dmaven.test.skip=true
cd scripts