#!/bin/bash
source dir.sh || exit
function ResetParameters() {
    app="GrepSum"
    checkpointInterval=20480
    tthread=20
    scheduler="OP_BFS_A"
    defaultScheduler="OP_BFS_A"
    CCOption=3 #TSTREAM
    complexity=0
    NUM_ITEMS=491520
    abort_ratio=0
    multiple_ratio=80
    txn_length=1
    NUM_ACCESS=2
    key_skewness=25
    isCyclic=1
    isDynamic=1
    workloadType="default,Up_abort,Down_abort,unchanging"
  # workloadType="default,unchanging,unchanging,unchanging,Up_abort,Down_abort,unchanging,unchanging"
  # workloadType="default,unchanging,unchanging,unchanging,Up_skew,Up_skew,Up_skew,Up_PD,Up_PD,Up_PD,Up_abort,Up_abort,Up_abort"
    schedulerPool="OP_BFS_A,OP_BFS"
    rootFilePath="${RSTDIR}"
    shiftRate=1
    multicoreEvaluation=0
    maxThreads=20
    totalEvents=`expr $checkpointInterval \* $tthread \* 4 \* $shiftRate`

    snapshotInterval=4
    arrivalControl=1
    arrivalRate=300
    FTOption=0
    isRecovery=0
    isFailure=0
    failureTime=25000
    measureInterval=100
    compressionAlg="None"
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
              --abort_ratio $abort_ratio \
              --multiple_ratio $multiple_ratio \
              --txn_length $txn_length \
              --NUM_ACCESS $NUM_ACCESS \
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
      --abort_ratio $abort_ratio \
      --multiple_ratio $multiple_ratio \
      --txn_length $txn_length \
      --NUM_ACCESS $NUM_ACCESS \
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
 app=GrepSum
 for FTOption in 3
 do
 #withoutRecovery
 withRecovery
 done
}
application_runner
