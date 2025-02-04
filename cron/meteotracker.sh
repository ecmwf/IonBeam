#!/usr/bin/env bash
cd /home/math/IonBeam
export ODC_ENABLE_WRITING_LONG_STRING_CODEC=1
echo >> cron/logs_meteotracker.txt
echo Meteotracker `date` >> cron/logs_meteotracker.txt
/home/math/.venv/bin/python -m ionbeam \
     ./config -vvvv \
    --env ewc \
    --sources  meteotracker \
    --download \
    --no-ingest-to-pipeline \
    --logfile cron/logs_meteotracker.txt