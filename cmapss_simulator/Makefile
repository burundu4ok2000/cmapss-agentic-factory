.PHONY: all terraform-init lint up down

all: terraform-init lint

terraform-init:
	cd terraform && terraform init

lint:
	echo "Linting..."

up:
	docker compose up -d

down:
	docker compose down
