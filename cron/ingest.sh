#!/usr/bin/env bash
cd /home/math/IonBeam
export ODC_ENABLE_WRITING_LONG_STRING_CODEC=1
echo >> cron/logs_ingest.txt
echo Ingestion `date` >> cron/logs_ingest.txt
/home/math/.venv/bin/python -m ionbeam \
     ./config -vvv \
    --env ewc \
    --sources  meteotracker smart_citizen_kit acronet \
    --no-download \
    --ingest-to-pipeline \
    --overwrite-fdb \
    --logfile cron/logs_ingest.txt