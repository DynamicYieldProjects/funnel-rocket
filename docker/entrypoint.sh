#!/usr/bin/env bash
case "$1" in
worker)
	echo "Starting Funnel Rocket queue-based worker"
	python -m frocket.worker.impl.queue_worker
	;;
apiserver)
	PORT=${APISERVER_PORT:-5000}
	NUM_WORKERS=${APISERVER_NUM_WORKERS:-8}
	echo "Starting Funnel Rocket API server with $NUM_WORKERS workers on port $PORT"
	python -m gunicorn.app.wsgiapp frocket.apiserver:app --bind=0.0.0.0:"$PORT" --workers="$NUM_WORKERS"
	;;
*)
	echo "Invalid command supplied"
	exit 1
	;;
esac
