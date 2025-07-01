#!/bin/bash

python_version=$1

command="pixi run -e test-${python_version} mypy src tests"

shift 1
if [ -n "$*" ]; then
    extra_args=$*
    command="$command $extra_args"
fi

echo "Running command: $command"
eval $command
