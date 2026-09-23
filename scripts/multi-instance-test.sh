#!/usr/bin/env bash

set -euo pipefail

COMPOSE_FILES=(
  -f docker-compose.yml
  -f docker-compose.multi.yml
)

echo "Building and starting three independent application containers..."
docker compose "${COMPOSE_FILES[@]}" up --build -d --wait app app2 app3

echo "Running multi-instance correctness harness three times..."
docker compose "${COMPOSE_FILES[@]}" --profile multi-test run --rm --no-deps multi-instance-test

echo "Multi-instance verification passed. The Compose stack remains running."
