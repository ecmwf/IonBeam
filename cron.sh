#!/usr/bin/env bash
cd /home/math/IonBeam
echo `date` >> cron_logs.txt
/home/math/.venv/bin/python -m ionbeam ./config --env=local --simple-output >> cron_logs.txt

# To drop into a debugger on error, enable the -simple-output option first
# python -m ionbeam ./config -vvvv --offline --finish-after=1 --simple-output --debug

# Or just insert breakpoint() into the code and use --simple-output