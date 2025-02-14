#!/usr/bin/env bash
cd /home/math/IonBeam/
export ODC_ENABLE_WRITING_LONG_STRING_CODEC=1
echo  >> cron/logs_acronet.txt
echo Acronet `date` >> cron/logs_acronet.txt
/home/math/.venv/bin/python -m ionbeam \
     ./config -vvvv \
    --env ewc \
    --sources  acronet \
    --download \
    --no-ingest-to-pipeline \
    --logfile cron/logs_acronet.txt