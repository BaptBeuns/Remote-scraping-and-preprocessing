#/bin/bash
PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin:/usr/games:/usr/local/games
export DISPLAY=:0.0
RUNNING_FILE="/home/reminiz/RUNNING.signal"
LOG_FILE="/home/reminiz/routine.log"
PASSWORD="YoBGdu93"

# The file RUNNING indicates if the process is running
if [ -e ${RUNNING_FILE} ] ; then
    echo "Process already running" >> ${LOG_FILE}
    exit 0
fi
touch "${RUNNING_FILE}"
echo "[CREATED] Running-signal file on "$(date)"." >> ${LOG_FILE}

# We start popping from the left, so that we serve the prior ones first.
CELEBRITY="$(redis-cli -h 40.118.3.19 -a "${PASSWORD}" LPOP to_scrap)"

while [ -n "${CELEBRITY}" ] ; do
    echo "[$(date)] Scraping ${CELEBRITY}" >> ${LOG_FILE}

    echo "[LAUNCHING]" /home/reminiz/scraping/routine_scraper.py "${CELEBRITY}" >> ${LOG_FILE}
    /usr/bin/python /home/reminiz/scraping/routine_scraper.py "${CELEBRITY}" >> ${LOG_FILE}

    echo "[OK] Pushing "${CELEBRITY}" to preproc" # >> ${LOG_FILE}
    redis-cli -h 40.118.3.19 -a "${PASSWORD}" RPUSH to_preproc "${CELEBRITY}"

    CELEBRITY="$(redis-cli -h 40.118.3.19 -a "${PASSWORD}" LPOP to_scrap)"
done

echo "[EXIT] Redis to_scrap list is empty." >> ${LOG_FILE}
rm -f "${RUNNING_FILE}"
echo "[DELETED] Running-signal file." >> ${LOG_FILE}
exit 0
