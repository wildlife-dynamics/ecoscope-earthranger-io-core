#!/bin/bash

python_version=$1

command="pixi run -e test-${python_version} pytest src tests --doctest-modules -vv"

shift 1
if [ -n "$*" ]; then
    extra_args=$*
    command="$command $extra_args"
fi

echo "Running command: $command"
eval $command
