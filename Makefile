.PHONY: up down verify logs

up:
	docker compose up -d --build

down:
	docker compose down

verify:
	./verify.sh $(GATE)

logs:
	docker compose logs -f
