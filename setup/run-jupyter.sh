#!/bin/bash
INTERNAL_PORT=8888
EXTERNAL_PORT=8889

#if [ -z "$STY" ]; then exec screen -dm -S tradingbot /bin/bash "$0"; fi
jupyter notebook --no-browser --port=${INTERNAL_PORT} --NotebookApp.allow_remote_access=1 &
sleep 5
socat TCP6-LISTEN:${EXTERNAL_PORT},fork TCP4:127.0.0.1:${INTERNAL_PORT} &