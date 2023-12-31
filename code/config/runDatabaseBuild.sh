#!/usr/bin/env bash

if [ $# -lt 1 ]; then
  echo "usage: $(basename $0) PROPS [OPT VAL [...]]" >&2
  exit 2
fi

PROPS="$1"
shift
if [ ! -f "${PROPS}" ]; then
  echo "${PROPS}: no such file or directory" >&2
  exit 1
fi

DB="$(grep '^db=' $PROPS | sed -e 's/^db=//')"
# Default
BEFORE_LOAD="tableCreates extraCommandsBeforeLoad storedProcedureCreates"

AFTER_LOAD="indexCreates foreignKeys extraHistID buildFinish"

for step in ${BEFORE_LOAD}; do
  ./runSQL.sh "${PROPS}" $step
done
echo "Create Table Mission Done."

./runLoader.sh "${PROPS}" $*
echo "Load Data Mission Done."

for step in ${AFTER_LOAD}; do
  ./runSQL.sh "${PROPS}" $step
done
echo "Index & Constraint Creat Mission Done."