#!/bin/bash
set -e
source venv/bin/activate

WORKFLOWS_DIR=$(realpath workflows)
if [[ $1 == '--workflows-dir' ]]; then
    WORKFLOWS_DIR=$(realpath "$2")
    shift; shift
fi

if [[ $1 == '--config' ]]; then
    export LUIGI_CONFIG_PATH=$(realpath "$2")
    shift; shift
fi

export PYTHONPATH=$WORKFLOWS_DIR

if [[ "$1" ]]; then
    MODULE=$1; shift
else
    echo "No module provided!"; exit 1
fi
if [[ "$1" ]]; then
    TASK=$1; shift
else
    echo "No task provided!"; exit 1
fi
echo LUIGI_CONFIG_PATH=$LUIGI_CONFIG_PATH
echo PYTHONPATH=$PYTHONPATH
for i in "$@"; do
    PARAMS="$PARAMS --${i%=*} ${i#*=}"
done

cd $WORKFLOWS_DIR
luigi --local-scheduler --module $MODULE $TASK$PARAMS
echo "exit: $?"
