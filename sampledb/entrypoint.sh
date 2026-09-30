#!/bin/sh
# Pin to CPUs 4-7 only if the host has them - taskset fails outright otherwise
if taskset -c 4-7 true 2>/dev/null; then
	set -- taskset -c 4-7 "$@"
fi
exec nice -n -20 "$@"
