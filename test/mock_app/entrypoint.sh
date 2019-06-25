#!/bin/sh

BASE=./scripts/data
IN=./work/input.json

if [ $(grep -c bogus $IN) -gt 0 ] ; then
   echo "Standard out line"
   echo "Standard error line"  >& 2
   FILE=output.json
elif [ $(grep -c voltest $IN) -gt 0 ] ; then
   echo "Standard out line"
   echo "Standard error line"  >& 2
   cat /staging/input.fa
   FILE=output.json
elif [ $(grep -c noout $IN) -gt 0 ] ; then
   exit 1
else
  echo "Error"
  FILE=error.json
  cp ${BASE}/${FILE} /kb/module/work/output.json
  exit 1
fi
P="${BASE}/${FILE}"

cp $P /kb/module/work/output.json
