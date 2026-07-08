# NYC Taxi Data Lakehouse — orchestration Docker (point d'entree unique)

# Prerequis : Docker Desktop + GNU Make (Git Bash ou WSL sur Windows)

#

#   cp docker/.env.example docker/.env

#   make up

#   make test-2026



COMPOSE   := docker compose -f docker/docker-compose.yml --env-file docker/.env

ENV_FILE  := docker/.env

BASH      := bash



.PHONY: help env up down restart status pipeline test-2026 logs clean clean-volumes urls



help:

	@echo "NYC Taxi Lakehouse — commandes disponibles"

	@echo ""

	@echo "  make env          Copie docker/.env.example -> docker/.env"

	@echo "  make up           Demarre la stack complete (HDFS + Spark + Mongo + Grafana)"

	@echo "  make down         Arrete et supprime les conteneurs"

	@echo "  make restart      down + up"

	@echo "  make pipeline     Pipeline complet (toutes les donnees raw)"

	@echo "  make test-2026    Pipeline filtre annee 2026 (tests soutenance)"

	@echo "  make status       Etat stack + HDFS + MongoDB"

	@echo "  make logs         Suivre le log pipeline"

	@echo "  make clean        down + prune Docker"

	@echo "  make clean-volumes Supprime les donnees HDFS/Mongo bind-mount"

	@echo "  make urls         Affiche les URLs (Grafana = KPIs + monitoring)"

	@echo ""



env:

	@if [ ! -f $(ENV_FILE) ]; then \

		cp docker/.env.example $(ENV_FILE); \

		echo "Cree $(ENV_FILE)"; \

	else \

		echo "$(ENV_FILE) existe deja"; \

	fi



up: env

	$(COMPOSE) --profile full up -d

	@$(BASH) scripts/docker/wait-ready.sh

	@$(MAKE) urls



down:

	$(COMPOSE) down --remove-orphans



restart: down up



pipeline: env

	@$(BASH) scripts/docker/pipeline.sh



test-2026: env

	@SAMPLE_YEAR=2026 $(BASH) scripts/docker/pipeline.sh --year 2026



status:

	python scripts/status.py



logs:

	@tail -f logs/pipeline_run.log



clean: down

	docker system prune -f



clean-volumes: down

	@echo "Suppression docker/volumes/ (HDFS, Mongo, Grafana)..."

	rm -rf docker/volumes/hdfs_datanode docker/volumes/hdfs_namenode docker/volumes/mongo docker/volumes/prometheus docker/volumes/grafana docker/volumes/spark-events docker/volumes/spark-shuffle

	mkdir -p docker/volumes/hdfs_datanode docker/volumes/hdfs_namenode docker/volumes/mongo docker/volumes/prometheus docker/volumes/grafana docker/volumes/spark-events docker/volumes/spark-shuffle



urls:

	@echo ""

	@echo "=== Services ==="

	@echo "  Grafana (KPIs + pipeline)  http://localhost:3000  (admin/admin)"

	@echo "    Dossier: NYC Lakehouse"

	@echo "    - 01 Pipeline (technique)"

	@echo "    - 02 KPIs metier (warehouse)"

	@echo "    - 03 Lake (ML, geo, anomalies)"

	@echo "  Spark Master UI           http://localhost:8080"

	@echo "  Spark History               http://localhost:18080"

	@echo "  HDFS NameNode               http://localhost:9870"

	@echo "  Prometheus                  http://localhost:9090"

	@echo ""

