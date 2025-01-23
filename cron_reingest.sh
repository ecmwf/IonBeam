#!/usr/bin/env bash
cd /home/math/IonBeam
export ODC_ENABLE_WRITING_LONG_STRING_CODEC=1
echo >> cron_logs_ingest.txt
echo Ingestion `date` >> cron_logs_ingest.txt
/home/math/.venv/bin/python -m ionbeam \
     ./config -vvv \
    --env ewc \
    --sources  acronet \
    --no-download \
    --ingest-to-pipeline \
    --overwrite-fdb \
    --reingest-from=2025-01-19 \
    --logfile cron_logs_ingest.txt