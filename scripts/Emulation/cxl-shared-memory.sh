#!/bin/bash
#
# Global functions commonly used for CXL-shared memory emulation
#
# Created by curry at 2022/12/05

#---------------------------------------------------------------

get_sysInfo (){
  uname -a
  echo "---------------------"
  sudo numactl -hardware
  echo "---------------------"
  lscpu
  echo "---------------------"
  cat /proc/meminfo
}

flush_fs_caches() {
  echo 3 | sudo tee /proc/sys/vm/drop_caches >/dev/null 2>&1
  sleep 10
}

disable_nmi_watchdog() {
  echo 0 | sudo tee /proc/sys/kernel/nmi_watchdog >/dev/null 2>&1
}

disable_turbo()
{
    echo 1 | sudo tee /sys/devices/system/cpu/intel_pstate/no_turbo >/dev/null 2>&1
}

enable_turbo()
{
    echo 0 | sudo tee /sys/devices/system/cpu/intel_pstate/no_turbo >/dev/null 2>&1
}

# 0: no randomization, everything is static
# 1: conservative randomization, shared libraries, stack, mmap(), VDSO and heap
# are randomized
# 2: full randomization, the above points in 1 plus brk()
disable_va_aslr()
{
    echo 0 | sudo tee /proc/sys/kernel/randomize_va_space >/dev/null 2>&1
}

disable_swap()
{
    sudo swapoff -a
}

disable_ksm()
{
    echo 0 | sudo tee /sys/kernel/mm/ksm/run >/dev/null 2>&1
}

disable_numa_balancing()
{
    echo 0 | sudo tee /proc/sys/kernel/numa_balancing >/dev/null 2>&1
}

# disable transparent hugepages
disable_thp()
{
    echo "never" | sudo tee /sys/kernel/mm/transparent_hugepage/enabled >/dev/null 2>&1
}

disable_ht()
{
    echo off | sudo tee /sys/devices/system/cpu/smt/control >/dev/null 2>&1
}

init_sys() {
   disable_nmi_watchdog
   disable_va_aslr
   disable_ksm
   disable_numa_balancing
   disable_thp
   disable_ht
   disable_turbo
   disable_swap
}

#---------------------------------------------------------------
# For Emon Run
#---------------------------------------------------------------

# $1: CXL experiment type array, EXL_EXPARR, (pass array by name!)
# $2: Base experiment type array
# $3: Result directory

init_emon_profiling() {
  # Attention: we are passing array name, and need convert it into an internal array format
  local cxl_exparr_name=$1[@]
  local cxl_exparr=( "${!cxl_exparr_name}" )
  local rstdir=$2

  mkdir -p $rstdir

  # CXL
  # ${#var} Calculate the length of the variable
    for ((et = 0; et < ${#cxl_exparr[@]}; et++)); do
        e=${cxl_exparr[$et]}
        if [[ $e == "no-share" ]]; then
            run_cmd="numactl --cpunodebind 0 --membind 0 -- bash ./cmd.sh"
        elif [[ $e == "CXL-shared" ]]; then
            run_cmd="numactl --cpunodebind 0 1 --membind 5 -- bash ./cmd.sh"
        elif [[ $e == "CXL-Interleave" ]]; then
            run_cmd="numactl --cpunodebind 0 --interleave=all -- bash ./cmd.sh"
        else
            echo "==> Error: unsupported experiment type: [$e]"
            exit
        fi

        echo "${run_cmd}" > $rstdir/emon-$e.sh
        chmod u+x $rstdir/emon-$e.sh
    done
}
# $1: CXL experiment type array, EXL_EXPARR, (pass array by name!)
# $2: Base experiment type array
cleanup_emon_profiling()
{
    # Attention: we are passing array name, and need convert it into an internal array format
    local cxl_exparr_name=$1[@]
    local cxl_exparr=( "${!cxl_exparr_name}" )

    # CXL
    for ((et = 0; et < ${#cxl_exparr[@]}; et++)); do
        e=${cxl_exparr[$et]}
        rm -rf emon-$e.sh
    done
}

# Run emon for one workload
# $1: experiment type, e.g. "L100", "CXL-Interleave", etc.
# $2: experiment id, e.g., "1", "2", etc.
# $3: result directory
# $4: memory footprint in MB (for running more splits)

run_emon_one() {
   local e=$1
   local id=$2
   local rstdir=$3
   local m=$4
   # log files
   local sysinfof=$rstdir/${e}-${id}-emon.sysinfo
   local pidstatf=$rstdir/${e}-${id}-emon.pidstat
   local memf=$rstdir/${e}-${id}-emon.mem
   local emonvf=$rstdir/${e}-${id}-emon.v
   local emonmf=$rstdir/${e}-${id}-emon.m
   local emondatf=$rstdir/${e}-${id}-emon.dat

   local epid

   flush_fs_caches

   get_sysInfo > $sysinfof 2>&1

   sudo emon -v > $emonvf
   sudo emon -M > $emonmf

   ./emon-${e}.sh > $rstdir/out-${e}-${id} &

   local c=$(basename ${PWD} | awk -F- '{print $1}')
   # $$ Get the current process ID
   # $! Get the previous process ID
   # $? Get the status code of the previous process (0 for success, 1 for failure)
   epid=$!
   echo "    => $e"

   sudo emon -i ${EMON_EVENT_FILE} -f "$emondatf" >/dev/null 2>&1 &
   pidstat -r -u -d -l -v -T ALL -p ALL -U -h 5 1000000 > $pidstatf &
   echo "Date Time Node0-Free-Mem-MB Node1-Free-Mem-MB" > $memf

   mpid=$!
   disown $mpid # avoid the "killed" message
   wait $epid
   sudo emon -stop
   killall pidstat >/dev/null 2>&1
   kill -9 $mpid >/dev/null 2>&1
}
