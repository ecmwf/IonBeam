#!/usr/bin/env bash
cd /home/math/IonBeam
export ODC_ENABLE_WRITING_LONG_STRING_CODEC=1
echo Meteotracker `date` >> cron_logs_meteotracker.txt
/home/math/.venv/bin/python -m ionbeam \
     ./config -vvvv \
    --env ewc \
    --sources  meteotracker \
    --logfile cron_logs_meteotracker.txt