# =========================
# CONFIG
# =========================
CONTAINER= simple-pipeline
DASHBOARD_CONTAINER= streamlit-app

# =========================
# DOCKER
# =========================
up:
	sudo docker compose up -d

build:
	sudo docker compose up -d --build

down:
	sudo docker compose down

restart:
	sudo docker compose down && docker compose up -d

logs:
	sudo docker compose logs -f

ps:
	sudo docker ps

# =========================
# PIPELINE
# =========================
run:
	sudo docker exec $(CONTAINER) bash run_pipeline.sh

shell:
	sudo docker exec -it $(CONTAINER) bash

# =========================
# DASHBOARD
# =========================
dashboard:
	sudo docker exec $(DASHBOARD_CONTAINER) streamlit run app/dashboard.py --server.port 8501 --server.address 0.0.0.0

# =========================
# DATABASE
# =========================
psql:
	sudo docker exec -it postgres-db psql -U admin -d crashdb

# =========================
# CLEAN
# =========================
clean:
	sudo rm -rf data/processed/*
