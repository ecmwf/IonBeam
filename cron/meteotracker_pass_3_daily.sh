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
    --ingest-to-pipeline \
    --logfile cron/logs_meteotracker_pass_3_daily.txt \
    --version=3 \
    --reingest \
    --time-span `date --date='3 days ago' --iso-8601=seconds` `date --date='1 days ago' --iso-8601=seconds`