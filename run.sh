#!/usr/bin/env bash
export ODC_ENABLE_WRITING_LONG_STRING_CODEC=1
python -m ionbeam ./config -vvvv \
    --sources  meteotracker acronet\
    --init-db \
    --env=local   #--overwrite --offline 
        # --finish-after=5 \


# To drop into a debugger on error, enable the -simple-output option first
# python -m ionbeam ./config -vvvv --offline --finish-after=1 --simple-output --debug

# Or just insert breakpoint() into the code and use --simple-output