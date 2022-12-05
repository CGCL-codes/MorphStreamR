#!/bin/bash
#
# Run CXL-shared memory experiments for dynamic workload
# Created by curry at 2022/12/05

BaseDir="/home/jjzhao/DTStream/scripts" # top dir of this repo
Dynamic_Run_dir="${BaseDir}/dynamic"
Rst_dir="${Dynamic_Run_dir}/rst/"

source BaseDir/Emulation/cxl-global.sh || exit

ini_sys
init_emon_profiling "no-share CXL-shared" ${Rst_dir}

