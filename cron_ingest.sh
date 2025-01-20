#!/usr/bin/env bash
cd /home/math/IonBeam
export ODC_ENABLE_WRITING_LONG_STRING_CODEC=1
echo >> cron_logs_ingest.txt
echo Meteotracker `date` >> cron_logs_ingest.txt
/home/math/.venv/bin/python -m ionbeam \
     ./config -vvv \
    --env ewc \
    --sources  meteotracker smart_citizen_kit \
    --no-download \
    --ingest-to-pipeline \
    --logfile cron_logs_ingest.txt