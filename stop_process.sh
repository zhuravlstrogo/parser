#!/bin/bash
if sudo kill -9 $(pgrep -f "chrome") ; then
    echo "Command succeeded"
else
    echo "Command failed"
fi