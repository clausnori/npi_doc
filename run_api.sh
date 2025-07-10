#!/bin/bash
DIR="$(cd "$(dirname "$0")" && pwd)"
if ! command -v which &> /dev/null; then
    if [ -x "$(command -v apt)" ]; then
        apt update && sudo apt install -y which
    elif [ -x "$(command -v yum)" ]; then
        yum install -y which
    elif [ -x "$(command -v pacman)" ]; then
        pacman -Sy which
    else
        echo "ERROR:Install 'which' "
        exit 1
    fi
fi

source app/bin/activate
uvicorn API.main:app --reload