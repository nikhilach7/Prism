#!/usr/bin/env bash
# Runs both MapReduce (Hadoop Streaming) jobs on the cluster and exports results to ./output
#   scripts/run_jobs.sh                 # normal run (with combiner)
#   scripts/run_jobs.sh --no-combiner   # for the combiner-vs-no-combiner timing comparison
set -euo pipefail
export MSYS_NO_PATHCONV=1   # Windows Git Bash: stop MSYS rewriting /... args before they hit docker.exe

THRESHOLD="${THRESHOLD:-10}"
USE_COMBINER=1
[[ "${1:-}" == "--no-combiner" ]] && USE_COMBINER=0

JAR=$(docker exec resourcemanager bash -c 'ls /opt/hadoop/share/hadoop/tools/lib/hadoop-streaming-*.jar | head -1')
mkdir -p output

run_job () {   # $1=name  $2=group-mode  $3=header
  local name="$1" mode="$2" header="$3"
  local out="/data/ecom/output/${name}"
  docker exec namenode hdfs dfs -rm -r -f "$out" >/dev/null 2>&1 || true

  local combiner_args=()
  [[ $USE_COMBINER -eq 1 ]] && combiner_args=(-combiner "python3 rr_reducer.py --combine")

  echo ">>> job: ${name}  (combiner=${USE_COMBINER})"
  local t0=$(date +%s)
  if [[ $USE_COMBINER -eq 1 ]]; then
  docker exec resourcemanager hadoop jar "$JAR" \
    -D mapreduce.job.name="returns_${name}" \
    -D mapreduce.job.reduces=1 \
    -D mapreduce.input.fileinputformat.input.dir.recursive=true \
    -file /opt/mapreduce/rr_mapper.py \
    -file /opt/mapreduce/rr_reducer.py \
    -input /data/ecom/raw \
    -output "$out" \
    -mapper "python3 rr_mapper.py --group ${mode}" \
    -combiner "python3 rr_reducer.py --combine" \
    -reducer "python3 rr_reducer.py --threshold ${THRESHOLD}"
  else
  docker exec resourcemanager hadoop jar "$JAR" \
    -D mapreduce.job.name="returns_${name}" \
    -D mapreduce.job.reduces=1 \
    -D mapreduce.input.fileinputformat.input.dir.recursive=true \
    -file /opt/mapreduce/rr_mapper.py \
    -file /opt/mapreduce/rr_reducer.py \
    -input /data/ecom/raw \
    -output "$out" \
    -mapper "python3 rr_mapper.py --group ${mode}" \
    -reducer "python3 rr_reducer.py --threshold ${THRESHOLD}"
  fi
  local secs=$(( $(date +%s) - t0 ))
  echo "${name},combiner=${USE_COMBINER},${secs}s" >> output/job_timings.csv

  { printf "%b\n" "$header"; docker exec namenode hdfs dfs -cat "${out}/part-*"; } > "output/${name}.tsv"
  echo "    exported output/${name}.tsv  ($(($(wc -l < output/${name}.tsv) - 1)) rows, ${secs}s)"
}

run_job week_category   week_category   "week\tcategory\torders\treturns\treturn_rate_pct\torder_value\treturned_value\tflag"
run_job category_region category_region "category\tregion\torders\treturns\treturn_rate_pct\torder_value\treturned_value\tflag"

echo "done. Check YARN UI http://localhost:8088 for counters (bad_records, combine input/output records)."
