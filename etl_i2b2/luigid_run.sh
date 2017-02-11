#!/bin/sh
#
# This configuration is intended for integration testing.
# ISSUE: production luigid configuration.

mkdir -p var  # for state
mkdir -p log
luigid --background --logdir=log --state-path=var/state.pickle --pidfile=var/luigid.pid

echo -n luigid pid:
cat var/luigid.pid
echo
