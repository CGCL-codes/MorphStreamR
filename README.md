<meta name="robots" content="noindex">

# Roronoa
## 1 Introduction
- This project aim at building a TSPE, which enables rapid recovery with minimal runtime overhead.
- Central to Roronoa is the principle of maintaining a selective historical view of resolved transaction dependencies over streams during runtime, thus enabling efficient parallel recovery.
- We evaluate Roronoa on varying workloads
## 2 Hardware Dependencies
- Roronoa is designed to run on a general-purpose multi-core CPE and does not require any special hardware.
- For optimal performance, we recommend using a machine with at least 24 cores and 300GB of memory.
- This configuration should be sufficient to run the Roronoa artifact effectively.
## 3 Software Dependencies 
- To ensure successful compilation, we recommend using a machine with Ubuntu 20.04 with JDK 1.8.0_301 and Mavean 3.8.1.
- Additionally, we set -Xmx and -Xms to be 300GB and use G!GC as the garbage collector arcoss all the experiments.
## 4 Experiment Workflow
### 4.1 Installation
- Once downloaded, you can use the provided scripts to compile the source code and install the JAR artifact by running the following command.
```
bash compile.sh
```
- The result and jar directory can be modified in dir.sh:
```
RSTDIR="/home/username/Benchmark/projectName"
```
```
JAR="/home/username/project/projectName/application/target/application-0.0.2-jar-with-dependencies.jar"
```
### 4.2 Recovery performance evaluation
- Execute the following command to evaluation the recovery performance and time breakdown for the corresponding application (application can be "gs", "tp", and "sl").
```
bash scripts/recovery/application-recovery-benchmark.sh
```
- Execute the following command to run factor analysis for corresponding application (application can be "gs", "tp", and "sl").
```
bash scripts/recovery/application-relax-benchmark.sh 
```
### 4.3 Runtime performance evaluation
- Execute the following command to evaluate runtime performance and system overhead for the corresponding application. (application can be "gs", "tp", and "sl").
```
bash scripts/runtime/application-runtime-benchmark.sh
```
- Execute the following command to evaluate the effectiveness of selective logging.
```
bash scripts/runtime/sl-run-selective-logging-benchmark.sh 
```
- Execute the following command to evaluate the effectiveness of workload-aware logging epoch.
```
bash scripts/runtime/sl-run-vary-epoch-benchmark.sh 
```
### 4.4 Scalability study
- Execute the following command to run the scalability experiment for the corresponding application. (application can be "gs", "tp", and "sl").
```
bash scripts/scalability/application-run-all-scalability-benchmark.sh
```
### 4.5 Workload sensitivity study
- Execute the following command to evaluate the impact of multi-partition state transaction.
```
bash scripts/sensitivity/gs-run-all-vary-multiple-benchmark.sh
```
- Execute the following command to evaluate the impact of state access skewness.
```
bash scripts/sensitivity/gs-run-all-vary-skew-benchmark.sh
```
- Execute the following command to evaluate the impact of aborting transactions.
```
bash scripts/sensitivity/gs-run-all-vary-abort-benchmark.sh
```
