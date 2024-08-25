# Makefile

# Define predetermined objectives
all: build clean init up
	@echo "Services created, Compose UP!"

build:
	docker compose build

clean:
	docker image prune -f

init:
	docker compose up airflow-init

up:
	docker compose up -d

# Terminate services and clean images and volumes
down:
	docker compose down --volumes --rmi all

clean: down
	@echo "Services completed and volumes cleaned"