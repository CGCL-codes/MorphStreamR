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
  workloadType="default,unchanging,unchanging,unchanging,Up_skew,Up_skew,Up_skew,Up_PD,Up_PD,Up_PD,Up_abort,Up_abort,Up_abort"
  schedulerPool="OG_BFS_A,OG_NS_A,OP_NS_A,OP_NS"
  rootFilePath="${RSTDIR}"
  shiftRate=1
  totalEvents=`expr $checkpointInterval \* $tthread \* 13 \* $shiftRate`

  snapshotInterval=2
  arrivalControl=1
  arrivalRate=200
  FTOption=2
  isRecovery=0
  isFailure=0
  failureTime=3000
  measureInterval=100
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
          --snapshotInterval $snapshotInterval \
          --arrivalControl $arrivalControl \
          --arrivalRate $arrivalRate \
          --FTOption $FTOption \
          --isRecovery $isRecovery \
          --isFailure $isFailure \
          --failureTime $failureTime \
          --measureInterval $measureInterval"
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
    --snapshotInterval $snapshotInterval \
    --arrivalControl $arrivalControl \
    --arrivalRate $arrivalRate \
    --FTOption $FTOption \
    --isRecovery $isRecovery \
    --isFailure $isFailure \
    --failureTime $failureTime \
    --measureInterval $measureInterval
}

function application_runner() { # multi-batch exp
 ResetParameters
 app=StreamLedger
 runApplication
}
application_runner