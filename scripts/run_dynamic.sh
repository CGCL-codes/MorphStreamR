#!/bin/bash
#
# Run CXL-shared memory experiments for dynamic workload
# Created by curry at 2022/12/05

#BaseDir="/Users/curryzjj/hair-loss/SC/DTStream/scripts" # top dir of this repo
BaseDir="/home/jjzhao/DTStream/scripts" # top dir of this repo
Emulation_dir="${BaseDir}/Emulation"
Dynamic_Run_dir="/dynamic"

source ${Emulation_dir}/cxl-shared-memory.sh || exit
cxl_exparr=('no-share' 'CXL-shared')
#ini_sys
init_emon_profiling "cxl_exparr" ${Emulation_dir}${Dynamic_Run_dir}

