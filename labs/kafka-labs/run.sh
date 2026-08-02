#!/usr/bin/env bash
# run.sh — compile and run one lab class.
#
#   ./run.sh ProducerBasic              # no arguments
#   ./run.sh Feed 100                   # with arguments
#   ./run.sh AvroConsumerApp lab05-v2   # with arguments
#
# Why this exists:
#   * `mvn exec:java` does NOT compile first, so editing a class and re-running it
#     silently executes the PREVIOUS version. This always compiles first.
#   * It checks your JDK before Maven produces a confusing error.
#   * It saves typing the fully-qualified class name every time.
#
# In an IDE you don't need this at all — just press Run on the class.

set -euo pipefail
cd "$(dirname "$0")"

PKG="com.elephantscale.kafka"

if [ $# -lt 1 ]; then
  echo "usage: ./run.sh <ClassName> [args...]" >&2
  echo "" >&2
  echo "available classes:" >&2
  find src/main/java -name '*.java' -exec basename {} .java \; 2>/dev/null | sort | sed 's/^/  /' >&2
  exit 2
fi

CLASS="$1"; shift
# accept either "ProducerBasic" or the fully-qualified name
case "$CLASS" in
  *.*) FQCN="$CLASS" ;;
  *)   FQCN="$PKG.$CLASS" ;;
esac

# --- JDK check: the single most common lab failure --------------------------
if ! command -v java >/dev/null 2>&1; then
  echo "ERROR: java not found. The labs need JDK 17." >&2
  exit 1
fi
jver=$(java -version 2>&1 | grep -oE '[0-9]+(\.[0-9]+)+' | command head -1)
jmaj=${jver%%.*}
[ "$jmaj" = "1" ] && jmaj=$(printf '%s' "$jver" | cut -d. -f2)   # legacy 1.8 -> 8
if [ -n "$jmaj" ] && [ "$jmaj" -lt 17 ] 2>/dev/null; then
  cat >&2 <<EOF
ERROR: java is $jver, but these labs need JDK 17.

  Linux:  sudo update-alternatives --config java
  macOS:  export JAVA_HOME=\$(/usr/libexec/java_home -v 17)
          export PATH="\$JAVA_HOME/bin:\$PATH"
  sdkman: sdk use java 17.0.12-amzn
EOF
  exit 1
fi

SRC="src/main/java/${PKG//.//}/${CLASS##*.}.java"
if [ ! -f "$SRC" ]; then
  echo "ERROR: $SRC not found." >&2
  echo "Save your lab class there, with 'package $PKG;' as its first line." >&2
  exit 1
fi

mvn -q -B compile

if [ $# -gt 0 ]; then
  mvn -q -B exec:java -Dexec.mainClass="$FQCN" -Dexec.args="$*"
else
  mvn -q -B exec:java -Dexec.mainClass="$FQCN"
fi
