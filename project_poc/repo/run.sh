#!/bin/bash
# Linux/macOS shell script equivalent of run.bat

set -e

help() {
    echo "Usage: ./run.sh [command]"
    echo ""
    echo "Commands:"
    echo "  start       - Start Airflow stack"
    echo "  stop        - Stop Airflow stack"
    echo "  restart     - Restart stack"
    echo "  logs        - Follow container logs"
    echo "  test        - Run pytest"
    echo "  lint        - Run flake8"
    echo "  pull-images - Pre-pull Docker images"
}

case "${1:-help}" in
    start)
        echo "Starting Airflow stack..."
        docker-compose -f docker/docker-compose.yaml up -d --build
        echo "Waiting for services..."
        sleep 20
        echo "Airflow UI: http://localhost:8080 (airflow/airflow)"
        ;;
    stop)
        echo "Stopping Airflow stack..."
        docker-compose -f docker/docker-compose.yaml down
        ;;
    restart)
        $0 stop
        $0 start
        ;;
    logs)
        echo "Tailing Airflow logs..."
        docker-compose -f docker/docker-compose.yaml logs -f
        ;;
    test)
        echo "Running tests..."
        pytest -q || code=$?
        if [ "$code" = "5" ]; then
            echo "No tests collected - OK"
        else
            exit $code
        fi
        ;;
    lint)
        echo "Running linter..."
        flake8 src tests
        ;;
    pull-images)
        echo "Pulling base images..."
        docker pull apache/airflow:2.10.0-python3.10
        ;;
    help|*)
        help
        ;;
esac
