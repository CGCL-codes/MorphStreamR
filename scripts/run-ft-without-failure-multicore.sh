#!/bin/bash
source dir.sh || exit
function ResetParameters() {
  app="StreamLedger"
  checkpointInterval=10240
  tthread=20
  scheduler="OG_BFS_A"
  defaultScheduler="OG_BFS_A"
  CCOption=3 #TSTREAM
  complexity=10000
  NUM_ITEMS=491520
  deposit_ratio=95
  key_skewness=0
  isCyclic=0
  isDynamic=1
  workloadType="default,unchanging,unchanging,unchanging,unchanging,unchanging,unchanging,unchanging,unchanging,unchanging,unchanging,unchanging"
  schedulerPool="OG_BFS_A,OG_NS_A,OP_NS_A,OP_NS"
  rootFilePath="${RSTDIR}"
  shiftRate=1
  multicoreEvaluation=1
  maxThreads=24
  totalEvents=`expr $checkpointInterval \* $tthread \* 12 \* $shiftRate`

  snapshotInterval=3
  arrivalControl=1
  arrivalRate=200
  FTOption=0
  isRecovery=0
  isFailure=0
  failureTime=3000
  measureInterval=100
  compressionAlg="RLE"
}

function runApplication() {
  echo "java -Xms300g -Xmx300g -jar -d64 ${JAR} \
            --app $app \
            --NUM_ITEMS $NUM_ITEMS \
            --tthread $tthread \
            --scheduler $scheduler \
            --defaultScheduler $defaultScheduler \
            --checkpoint_interval $checkpointInterval \
            --CCOption $CCOption \
            --complexity $complexity \
            --deposit_ratio $deposit_ratio \
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
  java -Xms300g -Xmx300g -Xss100M -XX:+PrintGCDetails -Xmn150g -XX:+UseG1GC -jar -d64 $JAR \
    --app $app \
    --NUM_ITEMS $NUM_ITEMS \
    --tthread $tthread \
    --scheduler $scheduler \
    --defaultScheduler $defaultScheduler \
    --checkpoint_interval $checkpointInterval \
    --CCOption $CCOption \
    --complexity $complexity \
    --deposit_ratio $deposit_ratio \
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
      runApplication
   done
}
function application_runner() {
 ResetParameters
 app=StreamLedger
 for FTOption in 0
 do
 multiCoreRunner
 done
}
application_runner