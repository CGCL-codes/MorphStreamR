#!/bin/bash
#
# Run CXL-shared memory experiments for dynamic workload
# Created by curry at 2022/12/05

BaseDir="/home/jjzhao/DTStream/scripts/" # top dir of this repo
Rst_dir="${BaseDir}/rst"
Emulation_dir="${BaseDir}/Emulation/rst"
Dynamic_Run_dir="${Rst_dir}/dynamic/"

source BaseDir/Emulation/cxl-shared-memory.sh || exit
cxl_exparr=('no-share' 'CXL-shared')
#ini_sys
init_emon_profiling "cxl_exparr" ${Emulation_dir}

