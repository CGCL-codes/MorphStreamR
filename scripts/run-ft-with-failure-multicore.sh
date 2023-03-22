#!/bin/bash
source dir.sh || exit
function ResetParameters() {
  app="StreamLedger"
  checkpointInterval=20480
  tthread=24
  scheduler="OG_BFS"
  defaultScheduler="OG_BFS"
  CCOption=3 #TSTREAM
  complexity=10000
  NUM_ITEMS=491520
  deposit_ratio=95
  abort_ratio=0
  key_skewness=25
  isCyclic=0
  isDynamic=1
  workloadType="default,unchanging,unchanging,unchanging,Up_abort,Down_abort,unchanging,unchanging"
#  workloadType="default,unchanging,unchanging,unchanging,Up_skew,Up_skew,Up_skew,Up_PD,Up_PD,Up_PD,Up_abort,Up_abort,Up_abort"
  schedulerPool="OG_BFS_A,OG_BFS"
  rootFilePath="${RSTDIR}"
  shiftRate=1
  multicoreEvaluation=1
  maxThreads=24
  totalEvents=`expr $checkpointInterval \* $maxThreads \* 8 \* $shiftRate`

  snapshotInterval=4
  arrivalControl=1
  arrivalRate=300
  FTOption=0
  isRecovery=0
  isFailure=0
  failureTime=12000
  measureInterval=100
  compressionAlg="None"
}

function runApplication() {
  echo "java -Xms300g -Xmx300g -Xss100M -XX:+PrintGCDetails -Xmn200g -XX:+UseG1GC -jar -d64 ${JAR} \
            --app $app \
            --NUM_ITEMS $NUM_ITEMS \
            --tthread $tthread \
            --scheduler $scheduler \
            --defaultScheduler $defaultScheduler \
            --checkpoint_interval $checkpointInterval \
            --CCOption $CCOption \
            --complexity $complexity \
            --deposit_ratio $deposit_ratio \
            --abort_ratio $abort_ratio \
            --key_skewness $key_skewness \
            --isCyclic $isCyclic \
            --rootFilePath $rootFilePath \
            --isDynamic $isDynamic \
            --totalEvents $totalEvents \
            --shiftRate $shiftRate \
            --workloadType $workloadType \
            --schedulerPool $schedulerPool \
            --multicoreEvaluation $multicoreEvaluation \
            --maxThreads $maxThreads \
            --snapshotInterval $snapshotInterval \
            --arrivalControl $arrivalControl \
            --arrivalRate $arrivalRate \
            --FTOption $FTOption \
            --isRecovery $isRecovery \
            --isFailure $isFailure \
            --failureTime $failureTime \
            --measureInterval $measureInterval \
            --compressionAlg $compressionAlg"
  java -Xms300g -Xmx300g -Xss100M -XX:+PrintGCDetails -Xmn200g -XX:+UseG1GC -jar -d64 $JAR \
    --app $app \
    --NUM_ITEMS $NUM_ITEMS \
    --tthread $tthread \
    --scheduler $scheduler \
    --defaultScheduler $defaultScheduler \
    --checkpoint_interval $checkpointInterval \
    --CCOption $CCOption \
    --complexity $complexity \
    --deposit_ratio $deposit_ratio \
    --abort_ratio $abort_ratio \
    --key_skewness $key_skewness \
    --isCyclic $isCyclic \
    --rootFilePath $rootFilePath \
    --isDynamic $isDynamic \
    --totalEvents $totalEvents \
    --shiftRate $shiftRate \
    --workloadType $workloadType \
    --schedulerPool $schedulerPool \
    --multicoreEvaluation $multicoreEvaluation \
    --maxThreads $maxThreads \
    --snapshotInterval $snapshotInterval \
    --arrivalControl $arrivalControl \
    --arrivalRate $arrivalRate \
    --FTOption $FTOption \
    --isRecovery $isRecovery \
    --isFailure $isFailure \
    --failureTime $failureTime \
    --measureInterval $measureInterval \
    --compressionAlg $compressionAlg
}

function multiCoreRunner() { # multi-batch exp
 for tthread in 24
   do
       isFailure=1
       isRecovery=0
       runApplication
       sleep 2s
       isFailure=0
       isRecovery=1
       runApplication
   done
}
function application_runner() {
 ResetParameters
 app=StreamLedger
 for FTOption in 1
 do
 multiCoreRunner
 done
}
application_runner