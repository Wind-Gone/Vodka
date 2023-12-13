#!/usr/bin/env bash

if [ $# -ne 1 ] ; then
    echo "usage: $(basename $0) PROPS_FILE" >&2
    exit 2
fi

SEQ_FILE="./.jTPCC_run_seq.dat"
if [ ! -f "${SEQ_FILE}" ] ; then
    echo "0" > "${SEQ_FILE}"
fi
SEQ=$(expr $(cat "${SEQ_FILE}") + 1) || exit 1
echo "${SEQ}" > "${SEQ_FILE}"

source funcs.sh $1

setCP || exit 1

myOPTS="-Dprop=$1 -DrunID=${SEQ}"
myOPTS="${myOPTS} -Djava.security.egd=file:/dev/./urandom"

# java -cp "$myCP" $myOPTS benchmark/oltp/OLTPClient
#java -cp-Xmx1G "$myCP" $myOPTS benchmark/oltp/OLTPClient
#java -cp "$myCP" $myOPTS benchmark/oltp/OLTPClient
java -Xmx20G -cp "$myCP" $myOPTS benchmark/oltp/OLTPClient
echo "end here in bash file"