#!/bin/bash
# Run entire pipeline on luigi server
# start luigid
python -m luigi --module main RunAll
# cmd "/c taskkill /IM "luigid.exe" /T /F"
