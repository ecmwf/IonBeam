#!/usr/bin/env bash
export ODC_ENABLE_WRITING_LONG_STRING_CODEC=1
python -m ionbeam ./config -vvvv \
    --env local \
    --sources meteotracker \
    --die-on-error  \
    --overwrite-fdb \
    --init-db 
        # --overwrite-fdb \



    # --finish-after=20 
    # --overwrite-cache 
    # --overwrite-fdb
    # --overwrite
    # --overwrite --offline 



# To drop into a debugger on error, enable the -simple-output option first
# python -m ionbeam ./config -vvvv --offline --finish-after=1 --simple-output --debug

# Or just insert breakpoint() into the code and use --simple-output