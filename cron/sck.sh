#!/usr/bin/env bash
cd /home/math/IonBeam
export ODC_ENABLE_WRITING_LONG_STRING_CODEC=1
echo >> cron/logs_smart_citizen_kit.txt
echo smart_citizen_kit `date` >> cron/logs_smart_citizen_kit.txt
/home/math/.venv/bin/python -m ionbeam \
     ./config -vvvv \
    --env ewc \
    --sources  smart_citizen_kit \
    --download \
    --no-ingest-to-pipeline \
    --overwrite-fdb \
    --logfile cron/logs_smart_citizen_kit.txt
    