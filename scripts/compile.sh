#!/bin/bash
export RUNDIR="/home/jjzhao/DTStream"
exprot RSTDIR="/home/jjzhao/Benchmark/DTStream"
export JAR="/home/jjzhao/DTStream/application/target/application-0.0.2-jar-with-dependencies.jar"
cd ..
mvn clean package
cd scripts