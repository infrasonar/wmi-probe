#!/bin/sh
# Raise the soft limit to the system maximum allowed hard limit
ulimit -n "$(ulimit -Hn)"
exec python main.py