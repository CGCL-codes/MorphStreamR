<meta name="robots" content="noindex">

# Roronoa
## Introduction
- This project aim at building a TSPE, which enable rapid recovery with minimal runtime overhead.
- Central to Roronoa is the principle of maintaining a selective historical view of resolved transaction dependencies over streams during runtime, thus enabling efficient parallel recovery.
- We evaluate Roronoa on varying workloads
## Running Roronoa
- All experiments scripts are in scripts/
- You can modify the result_dir and jar_dir in dir.sh
### Compile 
```
bash compile.sh
```
### Run overall experiment in section 6.2
```
bash scripts/gs-run-all-benchmark.sh. 

bash scripts/sl-run-all-benchmark.sh. 

bash scripts/tp-run-all-benchmark.sh. 

bash scripts/gs-run-relax-benchmark.sh. 

bash scripts/sl-run-relax-benchmark.sh. 

bash scripts/tp-run-relax-benchmark.sh. 

```
### Run breakdown analysis in section 6.3
> bash scripts/gs-run-all-benchmark.sh
> bash scripts/sl-run-all-benchmark.sh
> bash scripts/tp-run-all-benchmark.sh
### Run scalability study in section 6.4
> bash scripts/gs-run-all-scalability-benchmark.sh
> bash scripts/sl-run-all-scalability-benchmark.sh
> bash scripts/tp-run-all-scalability-benchmark.sh
### Run workload sensitivity study in section 6.5
> bash scripts/gs-run-all-vary-abort-benchmark.sh
> bash scripts/gs-run-all-vary-multiple-benchmark.sh
> bash scripts/gs-run-all-vary-skew-benchmark.sh
### Run overhead analysis in section 6.5
> bash scripts/sl-run-all-benchmark.sh
> bash scripts/sl-run-selective-logging-benchmark.sh
### Run other experiment
> bash scripts/sl-run-vary-epoch-benchmark.sh

