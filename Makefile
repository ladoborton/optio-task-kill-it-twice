.PHONY: up down seed verify logs

up:
	docker compose up -d --build

down:
	docker compose down

seed:
	./seed.sh

# make verify            all gates
# make verify GATE=G1    one gate
verify:
	./verify.sh $(GATE)

logs:
	docker compose logs -f
