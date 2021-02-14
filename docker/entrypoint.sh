#!/usr/bin/env bash
case "$1" in
  worker)
    echo "Starting Funnel Rocket queue-based worker"
    python -m frocket.worker.impl.queue_worker
    ;;
  apiserver)
    echo "Starting Funnel Rocket API server"
    FLASK_APP=frocket.apiserver python -m flask run --host=0.0.0.0
    ;;
  *)
    echo "Invalid command supplied"
    exit 1
    ;;
esac
