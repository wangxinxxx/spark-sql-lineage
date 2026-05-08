#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
Usage:
  bin/import-lineage-files-to-neo4j.sh [options] <lineage-file-or-dir>

Options:
  --uri URI          Neo4j URI. Default: neo4j://127.0.0.1:7687 or NEO4J_URI.
  --user USER        Neo4j user. Default: neo4j or NEO4J_USER.
  --password PASS    Neo4j password. Default: wx123456.. or NEO4J_PASSWORD.
  --skip-compile     Reuse existing target classes and classpath file.
  -h, --help         Show this help.

The input can be:
  output/sqlflow-debug/parallel-xxx/reports
  output/sqlflow-debug/parallel-xxx/reports/00001_xxx/lineage/lineage-graphs.jsonl
EOF
}

die() {
  echo "ERROR: $*" >&2
  exit 1
}

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$PROJECT_ROOT"

URI="${NEO4J_URI:-neo4j://127.0.0.1:7687}"
USER="${NEO4J_USER:-neo4j}"
PASSWORD="${NEO4J_PASSWORD:-wx123456..}"
SKIP_COMPILE=0
INPUT=""

while [ "$#" -gt 0 ]; do
  case "$1" in
    --uri)
      [ "$#" -ge 2 ] || die "$1 requires a value"
      URI="$2"
      shift 2
      ;;
    --user)
      [ "$#" -ge 2 ] || die "$1 requires a value"
      USER="$2"
      shift 2
      ;;
    --password)
      [ "$#" -ge 2 ] || die "$1 requires a value"
      PASSWORD="$2"
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
    -*)
      die "Unknown option: $1"
      ;;
    *)
      [ -z "$INPUT" ] || die "Only one input path is supported"
      INPUT="$1"
      shift
      ;;
  esac
done

[ -n "$INPUT" ] || {
  usage >&2
  exit 2
}
[ -e "$INPUT" ] || die "Input does not exist: $INPUT"

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
  env JAVA_HOME="${JAVA_HOME:-}" mvn -o -DskipTests test-compile \
    -Dmdep.outputFile="$CP_FILE" \
    -Dmdep.includeScope=runtime \
    dependency:build-classpath
fi

[ -f "$CP_FILE" ] || die "Classpath file not found: $CP_FILE. Run without --skip-compile first."

RUNTIME_CP="$PROJECT_ROOT/target/scala-2.12/classes:$PROJECT_ROOT/src/main/resources:$(cat "$CP_FILE")"
DEFAULT_LOG4J_CONFIG="-Dlog4j.configuration=file:$PROJECT_ROOT/src/main/resources/log4j-error.properties"
JAVA_OPTS="${JAVA_OPTS:--Xms256m -Xmx2048m -XX:MaxMetaspaceSize=384m -XX:ReservedCodeCacheSize=256m -Dfile.encoding=UTF-8 $DEFAULT_LOG4J_CONFIG}"

# JAVA_OPTS is intentionally split so callers can pass multiple JVM flags.
# shellcheck disable=SC2086
"$JAVA_BIN" $JAVA_OPTS \
  -cp "$RUNTIME_CP" \
  org.apache.spark.api.python.LineageFileToNeo4j \
  "$INPUT" \
  "$URI" \
  "$USER" \
  "$PASSWORD"

