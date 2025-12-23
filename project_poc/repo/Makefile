start:
	docker-compose -f docker/docker-compose.yaml up -d --build

dev:
	docker exec -it airflow-dev bash

run:
	docker exec airflow-dev python $(f)

stop:
	docker-compose -f docker/docker-compose.yaml down

restart: stop start

logs:
	docker-compose -f docker/docker-compose.yaml logs -f

lint:
	flake8 src tests

test:
	@pytest -q || code=$$?; if [ $$code -eq 5 ]; then \
		echo "No tests collected (treated as success)"; \
	else \
		exit $$code; \
	fi
