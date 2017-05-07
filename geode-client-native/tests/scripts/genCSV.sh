#!/bin/sh


showUsage()
{
  progName="`basename "$0"`"
  echo "Usage: $progName [-l] OUTPUT FILE..."
  echo " The first argument gives the output CSV file."
  echo " One of more files to be scanned must be provided on the command-line."
  echo " The optional -l option specifies that CSV for latency data should be"
  echo "  instead of the default perf data."
}

getPerfData()
{
  echo 'Region Tag,Task,NumClients,NumKeys,Value Size,NumThreads,ops/sec,Total ops,Time(in micros),Completion time'
  awk -F ',' '{
    if ($0 ~ /^\[PerfData\],/) {
      for (indx = 2; indx < NF; indx++) {
        if (indx == 4 && ($indx > 1000000 || $indx <= 0)) {
          printf "*unknown*,"
        } else {
          printf "%s,", $indx
        }
      }
      print $NF
    }
  }' "$@"
}

getLatencyData()
{
  echo 'Region Tag,MinLatency,MaxLatency,AvgLatency,No of samples'
  sed -n 's/^.*LatencyCSV,MinMaxAvgSamples,//p' "$@"
}


getLData=""
numArgs=0
while getopts ":l:" op; do
  case "${op}" in
    ("l") getLData="true" ;;
    (*) echo "Unknown argument -${OPTARG}." && showUsage && exit 1 ;;
  esac
  ((numArgs++))
done

while [ ${numArgs} -gt 0 ]; do
  shift
  ((numArgs--))
done

[ $# -lt 2 ] && showUsage && exit 1

outFile="$1"
shift

if [ -z "${getLData}" ]; then
  getPerfData "$@" > "${outFile}"
else
  getLatencyData "$@" > "${outFile}"
fi
