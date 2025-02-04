#!/usr/bin/env bash
cd /home/math/IonBeam
export ODC_ENABLE_WRITING_LONG_STRING_CODEC=1
echo >> cron_logs_ingest.txt
echo Ingestion `date` >> cron_logs_ingest.txt
/home/math/.venv/bin/python -m ionbeam \
     ./config -vvv \
    --env ewc \
    --sources  meteotracker \
    --download \
    --ingest-to-pipeline \
    --overwrite-fdb \
    --time-span 2025-01-01 2025-01-25 \
    --no-reingest \
    --logfile cron_logs_ingest.txt