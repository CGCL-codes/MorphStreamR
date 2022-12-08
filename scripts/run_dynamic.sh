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

if [[ $et == "L100" ]]; then
        run_cmd="numactl --cpunodebind 0 --membind 0 -- ""${run_cmd}"
    elif [[ $et == "L0" ]]; then
        run_cmd="numactl --cpunodebind 0 --membind 1 -- ""${run_cmd}"
    else
        # Other base splits (e.g. 90 means 90% of the workload memory will be
        # backed by local DRAM while the remaining 10% will be from CXL memory)
        run_cmd="numactl --cpunodebind 0 -- ${run_cmd}"
        #NODE0_TT_MEM=$(sudo numactl --hardware | grep 'node 0 size' | awk '{print $4}')
        NODE0_FREE_MEM=$(sudo numactl --hardware | grep 'node 0 free' | awk '{print $4}')
        ((NODE0_FREE_MEM -= 520))
        APP_MEM_ON_NODE0=$(echo "$mem*$et/100.0" | bc)
        MEM_SHOULD_RESERVE=$((NODE0_FREE_MEM - APP_MEM_ON_NODE0))
        MEM_SHOULD_RESERVE=${MEM_SHOULD_RESERVE%.*}
    fi

echo "===> MemEater reserving [$MEM_SHOULD_RESERVE] MB on Node 0..."
        if [[ $MEM_SHOULD_RESERVE -gt 0 ]]; then
            sudo killall memeater >/dev/null 2>&1
            sleep 10
            # Make sure that MemEater is reserving memory from Node 0
            numactl --cpunodebind 0 --membind 0 -- $MEMEATER ${MEM_SHOULD_RESERVE} &
            mapid=$!
            # Wait until memory eater consume all destined memory
            sleep 120
        fi

    run_one_exp "$w" "L100"
    run_one_exp "$w" "L0"
    # More Splits
    run_one_exp "$w" "95"
    run_one_exp "$w" "90"
    run_one_exp "$w" "85"
    run_one_exp "$w" "80"
    run_one_exp "$w" "75"
    run_one_exp "$w" "70"
    run_one_exp "$w" "60"
    run_one_exp "$w" "50"
    run_one_exp "$w" "40"
    run_one_exp "$w" "30"
    run_one_exp "$w" "25"