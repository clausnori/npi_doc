#!/bin/bash

#rm -rf temp

VENV_DIR="app"

python3 -m venv $VENV_DIR

source $VENV_DIR/bin/activate

pip install -r req.txt

echo "INSTALL OK '$VENV_DIR'"

# Shell DIR
DIR="$(cd "$(dirname "$0")" && pwd)"

SCRIPT_PATH="$DIR/update_npi.py"

CRON_TIME="0 3 * * 6"

TMP_CRON=$(mktemp)

crontab -l > "$TMP_CRON" 2>/dev/null

CRON_JOB="$CRON_TIME python $SCRIPT_PATH"

grep -Fx "$CRON_JOB" "$TMP_CRON" > /dev/null
if [ $? -ne 0 ]; then
    echo "$CRON_JOB" >> "$TMP_CRON"
    crontab "$TMP_CRON"
    echo "INIT CRON TASK: $CRON_JOB"
else
    echo "TASK ALREADY EXISTS"
fi

echo "PARSER INIT OK: $CRON_JOB"
rm "$TMP_CRON"

read -p "RUN UPDATE NOW NPI? (Y/N): " CONFIRM
if [[ "$CONFIRM" == "Y" || "$CONFIRM" == "y" ]]; then
    echo "Running update_npi.py..."
    python "$SCRIPT_PATH"
else
    echo "Skipped update_npi.py"
fi