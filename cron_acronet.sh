#!/usr/bin/env bash
cd /home/math/IonBeam
export ODC_ENABLE_WRITING_LONG_STRING_CODEC=1
echo Acronet `date` >> cron_logs_acronet.txt
/home/math/.venv/bin/python -m ionbeam \
     ./config -vvvv \
    --env ewc \
    --sources  acronet \
    --logfile cron_logs_acronet.txt