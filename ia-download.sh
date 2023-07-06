#!/bin/bash
set -euxo pipefail

COLLECTION=${1}
OUTPUT_DIR=${2}
LOG_DIR=${3}
JOBS=${4}
IA_CONFIG=${5}

if test -f "$IA_CONFIG"; then
    echo "Using $IA_CONFIG as config"
else
    echo "ERROR: Config $IA_CONFIG doesn't exist, exiting!"
    exit
fi

mkdir -p ${LOG_DIR}/${COLLECTION}
ITEMLIST=${LOG_DIR}/${COLLECTION}/items.txt
if test -f "$ITEMLIST"; then
    echo "Found already downloaded itemlist: $ITEMLIST"
else
    echo "Downloading itemlist to $ITEMLIST"
    ia search -i collection:${COLLECTION} >${ITEMLIST}.tmp
    mv ${ITEMLIST}.tmp ${ITEMLIST}
fi

SCRIPT_DIR=$(dirname ${0})

export PYTHONUNBUFFERED=1
export IA_CONFIG_FILE=$IA_CONFIG

DATE=$(date +'%Y%m%d%H%M%S')

cnt=1
until python ${SCRIPT_DIR}/ia-download.py --cache ${LOG_DIR}/${COLLECTION}/cache.db --shuffle --dest ${OUTPUT_DIR}/${COLLECTION} --jobs $JOBS <${ITEMLIST} >${LOG_DIR}/${COLLECTION}/attempt_${cnt}.${DATE}.stdout 2>${LOG_DIR}/${COLLECTION}/attempt_${cnt}.${DATE}.stderr ; do
    echo "Attempt $cnt: not all files were downloaded, see ${LOG_DIR}/${COLLECTION}/attempt_${cnt}.${DATE}.stderr for errors. Retrying..."
    let cnt++
done
echo "Attempt $cnt: all files were downloaded. See ${LOG_DIR}/${COLLECTION}/attempt_${cnt}.${DATE}.* for logs. "





