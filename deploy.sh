#!/bin/bash
# Production deploy script
# Usage: ./deploy.sh
set -euo pipefail

echo "=== ULTRA Fencing Club — Production Deploy ==="

# Проверяем наличие .env.production
if [ ! -f ".env.production" ]; then
  echo "ERROR: .env.production not found. Copy it to the server manually."
  exit 1
fi

# Подтягиваем последние изменения
git pull origin main

# Пересобираем образы
docker compose -f docker-compose.prod.yml build --no-cache

# Останавливаем старые контейнеры, поднимаем новые
docker compose -f docker-compose.prod.yml up -d

# Ждём БД
echo "Waiting for database..."
sleep 5

# Применяем миграции
docker compose -f docker-compose.prod.yml exec admin alembic upgrade head

echo "=== Deploy complete ==="
docker compose -f docker-compose.prod.yml ps
