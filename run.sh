#!/usr/bin/env bash
python -m ionbeam ./config -vvvv --env=local --init-db #--overwrite --offline 

# To drop into a debugger on error, enable the -simple-output option first
# python -m ionbeam config/ionbeam -vvvv --offline --finish-after=5 --simple-output --debug

# Or just insert breakpoint() into the code and use --simple-output