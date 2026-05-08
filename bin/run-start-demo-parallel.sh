#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
Usage:
  bin/run-start-demo-parallel.sh [options] <sql-file-or-dir> [...]

Options:
  -j, --jobs N        Parallel JVM tasks. Default: 5.
  -l, --log-dir DIR   Log root directory. Default: output/sqlflow-debug/parallel-<timestamp>.
  --skip-compile      Reuse existing target classes and classpath file.
  -h, --help          Show this help.

Each SQL file runs in an independent JVM process:
  java -cp ... org.apache.spark.api.python.StartDemo <sql-file> <task-output-dir>

Outputs under log-dir:
  logs/               Per-task stdout and stderr.
  reports/            Per-task StartDemo report files.
  status.tsv          Per-task final status summary.
  failures.tsv        Failed task summary.
  maven.log           Compile/classpath preparation log.

Environment:
  JAVA_OPTS           JVM options for each task. Default:
                      -Xms256m -Xmx2048m -XX:MaxMetaspaceSize=384m
                      -XX:ReservedCodeCacheSize=256m -Dfile.encoding=UTF-8
                      -Dlog4j.configuration=file:<project>/src/main/resources/log4j-error.properties
EOF
}

timestamp() {
  date '+%Y%m%d-%H%M%S'
}

log_time() {
  date '+%Y-%m-%d %H:%M:%S'
}

die() {
  echo "ERROR: $*" >&2
  exit 1
}

sanitize_name() {
  local name="$1"
  printf '%s' "$name" |
    sed 's#[/:]#_#g; s#[[:cntrl:]]#_#g' |
    cut -c 1-180
}

escape_tsv() {
  printf '%s' "$1" | tr '\t\r\n' '   '
}

active_jobs() {
  jobs -pr | wc -l | tr -d ' '
}

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$PROJECT_ROOT"

JOBS=5
LOG_ROOT=""
SKIP_COMPILE=0
INPUTS=()

while [ "$#" -gt 0 ]; do
  case "$1" in
    -j|--jobs)
      [ "$#" -ge 2 ] || die "$1 requires a value"
      JOBS="$2"
      shift 2
      ;;
    -l|--log-dir)
      [ "$#" -ge 2 ] || die "$1 requires a value"
      LOG_ROOT="$2"
      shift 2
      ;;
    --skip-compile)
      SKIP_COMPILE=1
      shift
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    --)
      shift
      while [ "$#" -gt 0 ]; do
        INPUTS+=("$1")
        shift
      done
      ;;
    -*)
      die "Unknown option: $1"
      ;;
    *)
      INPUTS+=("$1")
      shift
      ;;
  esac
done

[ "${#INPUTS[@]}" -gt 0 ] || {
  usage >&2
  exit 2
}

case "$JOBS" in
  ''|*[!0-9]*)
    die "--jobs must be a positive integer"
    ;;
esac
[ "$JOBS" -gt 0 ] || die "--jobs must be greater than 0"

if [ -z "$LOG_ROOT" ]; then
  LOG_ROOT="$PROJECT_ROOT/output/sqlflow-debug/parallel-$(timestamp)"
elif [[ "$LOG_ROOT" != /* ]]; then
  LOG_ROOT="$PROJECT_ROOT/$LOG_ROOT"
fi

LOG_DIR="$LOG_ROOT/logs"
REPORT_DIR="$LOG_ROOT/reports"
STATUS_DIR="$LOG_ROOT/status"
mkdir -p "$LOG_DIR" "$REPORT_DIR" "$STATUS_DIR"

SQL_FILES=()
for input in "${INPUTS[@]}"; do
  if [ -d "$input" ]; then
    while IFS= read -r file; do
      SQL_FILES+=("$file")
    done < <(find "$input" -maxdepth 1 -type f -name '*.sql' | sort)
  elif [ -f "$input" ]; then
    case "${input##*.}" in
      sql|SQL)
        SQL_FILES+=("$input")
        ;;
      *)
        die "Not a .sql file: $input"
        ;;
    esac
  else
    die "Input does not exist: $input"
  fi
done

[ "${#SQL_FILES[@]}" -gt 0 ] || die "No .sql files found"

if [ -z "${JAVA_HOME:-}" ] &&
    [ -d "/Users/zz/Library/Java/JavaVirtualMachines/corretto-1.8.0_482/Contents/Home" ]; then
  export JAVA_HOME="/Users/zz/Library/Java/JavaVirtualMachines/corretto-1.8.0_482/Contents/Home"
fi

if [ -n "${JAVA_HOME:-}" ]; then
  JAVA_BIN="$JAVA_HOME/bin/java"
else
  JAVA_BIN="java"
fi

CP_FILE="$PROJECT_ROOT/target/sqlflow-runtime.classpath"
if [ "$SKIP_COMPILE" -eq 0 ]; then
  echo "[$(log_time)] Preparing classes and runtime classpath..."
  env JAVA_HOME="${JAVA_HOME:-}" mvn -o -DskipTests test-compile \
    -Dmdep.outputFile="$CP_FILE" \
    -Dmdep.includeScope=runtime \
    dependency:build-classpath \
    > "$LOG_ROOT/maven.log" 2>&1 || {
      echo "Maven preparation failed. See: $LOG_ROOT/maven.log" >&2
      exit 1
    }
fi

[ -f "$CP_FILE" ] || die "Classpath file not found: $CP_FILE. Run without --skip-compile first."

RUNTIME_CP="$PROJECT_ROOT/target/scala-2.12/classes:$PROJECT_ROOT/src/main/resources:$(cat "$CP_FILE")"
DEFAULT_LOG4J_CONFIG="-Dlog4j.configuration=file:$PROJECT_ROOT/src/main/resources/log4j-error.properties"
JAVA_OPTS="${JAVA_OPTS:--Xms256m -Xmx2048m -XX:MaxMetaspaceSize=384m -XX:ReservedCodeCacheSize=256m -Dfile.encoding=UTF-8 $DEFAULT_LOG4J_CONFIG}"

run_one() {
  local index="$1"
  local sql_file="$2"
  local base
  local safe
  local task_name
  local task_report_dir
  local log_file
  local status_file
  local start_time
  local end_time
  local exit_code
  local status
  local failure_line

  base="$(basename "$sql_file")"
  safe="$(sanitize_name "$base")"
  task_name="$(printf '%05d_%s' "$index" "$safe")"
  task_report_dir="$REPORT_DIR/$task_name"
  log_file="$LOG_DIR/$task_name.log"
  status_file="$STATUS_DIR/$task_name.status"
  mkdir -p "$task_report_dir"

  start_time="$(log_time)"
  {
    echo "[$start_time] START index=$index file=$sql_file"
    echo "[$start_time] REPORT_DIR=$task_report_dir"
    echo "[$start_time] JAVA_OPTS=$JAVA_OPTS"
  } > "$log_file"

  set +e
  # JAVA_OPTS is intentionally split so callers can pass multiple JVM flags.
  # shellcheck disable=SC2086
  "$JAVA_BIN" $JAVA_OPTS \
    -cp "$RUNTIME_CP" \
    org.apache.spark.api.python.StartDemo \
    "$sql_file" \
    "$task_report_dir" \
    >> "$log_file" 2>&1
  exit_code="$?"
  set -e

  end_time="$(log_time)"
  if [ "$exit_code" -ne 0 ]; then
    status="PROCESS_FAILURE"
  elif grep -q '^\[SCRIPT\] status=FAILURE' "$log_file"; then
    status="SCRIPT_FAILURE"
  else
    status="SUCCESS"
  fi

  failure_line="$(grep -m 1 '^\[SCRIPT-FAILURE\]' "$log_file" || true)"
  {
    echo "[$end_time] END index=$index status=$status exit_code=$exit_code"
  } >> "$log_file"

  printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n' \
    "$index" \
    "$status" \
    "$exit_code" \
    "$(escape_tsv "$start_time")" \
    "$(escape_tsv "$end_time")" \
    "$(escape_tsv "$sql_file")" \
    "$(escape_tsv "$log_file")" \
    "$(escape_tsv "$task_report_dir")" \
    "$(escape_tsv "$failure_line")" \
    > "$status_file"

  echo "[$end_time] $status index=$index file=$base log=$log_file"
  return 0
}

echo "[$(log_time)] SQL files: ${#SQL_FILES[@]}"
echo "[$(log_time)] Parallel jobs: $JOBS"
echo "[$(log_time)] Log root: $LOG_ROOT"

index=0
for sql_file in "${SQL_FILES[@]}"; do
  index=$((index + 1))
  while [ "$(active_jobs)" -ge "$JOBS" ]; do
    sleep 1
  done
  run_one "$index" "$sql_file" &
done

wait

STATUS_FILE="$LOG_ROOT/status.tsv"
FAILURES_FILE="$LOG_ROOT/failures.tsv"
{
  echo "index	status	exit_code	start_time	end_time	sql_file	log_file	report_dir	first_failure"
  find "$STATUS_DIR" -type f -name '*.status' | sort | while IFS= read -r file; do
    cat "$file"
  done
} > "$STATUS_FILE"

awk -F '\t' 'NR == 1 || $2 != "SUCCESS"' "$STATUS_FILE" > "$FAILURES_FILE"

total_count="${#SQL_FILES[@]}"
success_count="$(awk -F '\t' 'NR > 1 && $2 == "SUCCESS" { c++ } END { print c + 0 }' "$STATUS_FILE")"
failure_count=$((total_count - success_count))

echo "[$(log_time)] DONE total=$total_count success=$success_count failure=$failure_count"
echo "[$(log_time)] Status: $STATUS_FILE"
echo "[$(log_time)] Failures: $FAILURES_FILE"
echo "[$(log_time)] Logs: $LOG_DIR"
