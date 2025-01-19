#!/usr/bin/env bash
cd /home/math/IonBeam
export ODC_ENABLE_WRITING_LONG_STRING_CODEC=1
echo Acronet `date` >> smart_citizen_kit
/home/math/.venv/bin/python -m ionbeam \
     ./config -vvvv \
    --env ewc \
    --sources  smart_citizen_kit \
    --logfile cron_logs_smart_citizen_kit.txt