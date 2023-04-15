#!/bin/bash
source dir.sh || exit
function ResetParameters() {
    app="StreamLedger"
    checkpointInterval=20480
    tthread=24
    scheduler="OP_BFS_A"
    defaultScheduler="OP_BFS_A"
    CCOption=3 #TSTREAM
    complexity=0
    NUM_ITEMS=491520
    deposit_ratio=65
    abort_ratio=0
    key_skewness=25
    isCyclic=1
    isDynamic=1
    workloadType="default,Up_abort,Down_abort,unchanging"
  # workloadType="default,unchanging,unchanging,unchanging,Up_abort,Down_abort,unchanging,unchanging"
  # workloadType="default,unchanging,unchanging,unchanging,Up_skew,Up_skew,Up_skew,Up_PD,Up_PD,Up_PD,Up_abort,Up_abort,Up_abort"
    schedulerPool="OP_BFS_A,OP_BFS"
    rootFilePath="${RSTDIR}"
    shiftRate=1
    multicoreEvaluation=1
    maxThreads=24
    totalEvents=`expr $checkpointInterval \* $maxThreads \* 4 \* $shiftRate`

    snapshotInterval=4
    arrivalControl=1
    arrivalRate=300
    FTOption=0
    isRecovery=0
    isFailure=0
    failureTime=12000
    measureInterval=100
    compressionAlg="Dictionary"
    isSelective=0
    maxItr=0
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
              --compressionAlg $compressionAlg \
              --isSelective $isSelective \
              --maxItr $maxItr"
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
      --compressionAlg $compressionAlg \
      --isSelective $isSelective \
      --maxItr $maxItr
}
function withRecovery() {
    isFailure=1
    isRecovery=0
    runApplication
    sleep 2s
    isFailure=0
    isRecovery=1
    runApplication
}
function withoutRecovery() {
  runApplication
  sleep 2s
}

function application_runner() {
 ResetParameters
 app=StreamLedger
 for FTOption in 0 1 2 3 4 5
 do
 #withoutRecovery
 withRecovery
 done
}
application_runner
