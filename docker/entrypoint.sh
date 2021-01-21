#!/usr/bin/env bash

case "$1" in
  worker)
    echo "Starting frocket task worker"
    python /app/frocket/worker/impl/queue_worker.py
    ;;
  apiserver)
    echo "Starting frocket api server"
    python -m flask run --host=0.0.0.0
    ;;
  *)
    # The command is something like bash, not an frocket subcommand. Just run it in the right environment.
    echo "Invalid command supplied"
    exec "$@"
    ;;
esac
