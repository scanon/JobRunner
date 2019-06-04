#!/bin/sh

BASE=./scripts/data
IN=./work/input.json

if [ $(grep -c bogus $IN) -gt 0 ] ; then
   echo "Standard out line"
   echo "Standard error line"  >& 2
   FILE=output.json
else
  echo "Error"
  FILE=error.json
  cp ${BASE}/${FILE} /kb/module/work/output.json
  exit 1
fi
P="${BASE}/${FILE}"

cp $P /kb/module/work/output.json
