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
    --logfile cron/logs_meteotracker_pass_2_hourly.txt \
    --version=2 \
    --time-span `date --date='1 days ago' --iso-8601=seconds` `date --date='1 hours ago' --iso-8601=seconds` \
    --reingest