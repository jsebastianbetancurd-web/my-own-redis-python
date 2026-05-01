#!/bin/sh
# 
# redis-server.sh - Entry point for the Python Redis Clone
#
# Use this script to launch the server locally. 
# Usage: ./redis-server.sh [--port <port>] [--replicaof "<master-host> <master-port>"]

set -e # Exit early if any commands fail

# Launch the server using 'uv' to manage dependencies and runtime
exec uv run --quiet -m app.main "$@"
