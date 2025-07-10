#!/bin/bash

# Shell DIR
DIR="$(cd "$(dirname "$0")" && pwd)"

SCRIPT_PATH="$DIR/update_npi.py"
#Python patch

PYTHON=$(which python3)

CRON_TIME="0 3 * * *"

TMP_CRON=$(mktemp)

crontab -l > "$TMP_CRON" 2>/dev/null

CRON_JOB="$CRON_TIME $PYTHON $SCRIPT_PATH"

grep -Fx "$CRON_JOB" "$TMP_CRON" > /dev/null
if [ $? -ne 0 ]; then
    echo "$CRON_JOB" >> "$TMP_CRON"
    crontab "$TMP_CRON"
    echo "INIT CRON TASK: $CRON_JOB"
else
    echo "TASK ALREADY EXISTS"
fi

rm "$TMP_CRON"