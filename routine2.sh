#/bin/bash
PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin:/usr/games:/usr/local/games

RUNNING_FILE="/disk2/tonio/scrap_type_2/Process_running.signal"
LOG_FILE="/disk2/tonio/scrap_type_2/Preprocessing.log"
PASSWORD="YoBGdu93"

# The file RUNNING indicates if the process is running
if [ -e ${RUNNING_FILE} ] ; then
    echo "Process already running" >> ${LOG_FILE}
    exit 0
fi
touch "${RUNNING_FILE}"
echo "[CREATED] Running-signal file on "$(date)"." >> ${LOG_FILE}

# We start popping from the left, so that we serve the prior ones first.
CELEBRITY="$(redis-cli -h 40.118.3.19 -a "${PASSWORD}" LPOP to_preproc)"

while [ -n "${CELEBRITY}" ] ; do
    echo "[$(date)] Preprocessing ${CELEBRITY}" >> ${LOG_FILE}

    echo python /disk2/tonio/scrap_type_2/routine_preproc.py \"${CELEBRITY}\" >> ${LOG_FILE}
    python /disk2/tonio/scrap_type_2/routine_preproc.py "${CELEBRITY}" >> ${LOG_FILE}

    CELEBRITY="$(redis-cli -h 40.118.3.19 -a "${PASSWORD}" LPOP to_preproc)"
done

echo "[EXIT] Redis to_preproc list is empty." >> ${LOG_FILE}
rm -f "${RUNNING_FILE}"
echo "[DELETED] Running-signal file." >> ${LOG_FILE}
exit 0
