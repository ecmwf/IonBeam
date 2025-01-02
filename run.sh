#!/usr/bin/env bash
export ODC_ENABLE_WRITING_LONG_STRING_CODEC=1
python -m ionbeam ./config -vvvv \
    --env local \
    --sources acronet \
    --die-on-error
    # --finish-after=2 \
    # --init-db \
    # --overwrite-cache 
    
    
        # --finish-after=5 \
    
    

    # --overwrite-cache 
    # --overwrite-fdb
    # --init-db
        # --finish-after=3 \
    # --overwrite
   
    
    # --finish-after=3
    
    # --env=local   #--overwrite --offline 
        # --finish-after=5 \
            # --init-db \


# To drop into a debugger on error, enable the -simple-output option first
# python -m ionbeam ./config -vvvv --offline --finish-after=1 --simple-output --debug

# Or just insert breakpoint() into the code and use --simple-output