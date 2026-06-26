# Soma — local dev + cluster workflow.
#
# `make up` builds (if needed) and starts the full three-role cluster
# (one meta, three storage, one gateway) from deploy/compose. The S3
# endpoint is at http://127.0.0.1:9000 once `make ready` flips green;
# admin (health/metrics) is at http://127.0.0.1:9001.
#
# `make run` starts a single standalone node in Docker (all roles in one
# process) — the quickest S3 endpoint at http://127.0.0.1:9000.
#
# Run `make help` to list every target.

COMPOSE    ?= docker compose -f deploy/compose/docker-compose.yml
EC_COMPOSE ?= docker compose -f deploy/compose/docker-compose.yml -f deploy/compose/docker-compose.erasure.yml
S3_URL     ?= http://127.0.0.1:9000
ADMIN_URL  ?= http://127.0.0.1:9001
IMAGE      ?= soma:dev

.DEFAULT_GOAL := help

.PHONY: help \
        build test lint fmt fmt-check check run \
        image up down restart rebuild logs ps sh clean \
        ready health metrics smoke \
        ec-up ec-down ec-clean ec-ps ec-logs ec-degraded

help: ## Show this help
	@printf "Soma targets:\n\n"
	@awk 'BEGIN {FS = ":.*?## "} /^[a-zA-Z][a-zA-Z_-]*:.*?## / {printf "  \033[36m%-12s\033[0m %s\n", $$1, $$2}' $(MAKEFILE_LIST)
	@printf "\nCluster S3: $(S3_URL)   Admin: $(ADMIN_URL)\n"

# ── dev (Rust workspace) ───────────────────────────────────────────────────

build: ## Compile the whole workspace (debug)
	cargo build --workspace

test: ## Run the whole test suite
	cargo test --workspace

lint: ## Clippy across all crates and targets
	cargo clippy --workspace --all-targets

fmt: ## Format the workspace in place
	cargo fmt --all

fmt-check: ## Check formatting without writing
	cargo fmt --all --check

check: fmt-check lint test ## CI gate: fmt + clippy + tests

run: image ## Run a single standalone node in Docker (S3 on :9000, admin :9001)
	docker run --rm -it -p 9000:9000 -p 9001:9001 \
		-v soma-standalone:/var/lib/soma --name soma-standalone $(IMAGE)

# ── local cluster (Docker Compose) ─────────────────────────────────────────

image: ## Build the single soma-server image (all roles)
	docker build -t $(IMAGE) .

up: ## Build (if needed) and start the meta + 3 storage + gateway cluster
	$(COMPOSE) up -d --build
	@printf "\nCluster starting — gateway waits for 3 storage nodes to register.\n"
	@printf "Check readiness: make ready\n"
	@printf "Tail logs:       make logs\n"

down: ## Stop and remove the cluster (KEEPS data volumes)
	$(COMPOSE) down

restart: ## Restart the cluster without rebuilding
	$(COMPOSE) restart

rebuild: ## Rebuild the image with no layer cache
	$(COMPOSE) build --no-cache

logs: ## Tail cluster logs (Ctrl-C to detach)
	$(COMPOSE) logs -f --tail=200

ps: ## Show cluster container status
	$(COMPOSE) ps

sh: ## Open a shell in the gateway container
	$(COMPOSE) exec gateway sh

clean: ## Stop the cluster and DELETE its data volumes
	$(COMPOSE) down -v --remove-orphans

# ── probes / smoke test ────────────────────────────────────────────────────

ready: ## Wait for the gateway to report ready (readyz)
	@curl -fsS --retry-connrefused --retry 40 --retry-delay 1 $(ADMIN_URL)/readyz \
		&& printf "  cluster ready ($(S3_URL))\n" \
		|| (printf "\nGateway not ready. Is the cluster up? Try \`make logs\`.\n" && exit 1)

health: ## Probe the gateway liveness endpoint (healthz)
	@curl -fsS $(ADMIN_URL)/healthz && printf "  ok\n" \
		|| (printf "\nHealthcheck failed. Try \`make logs\`.\n" && exit 1)

metrics: ## Print gateway Prometheus metrics
	@curl -fsS $(ADMIN_URL)/metrics || (printf "\nMetrics unreachable. Try \`make logs\`.\n" && exit 1)

smoke: ## S3 create/put/get/delete roundtrip against the running cluster (needs python3 + boto3)
	@python3 - <<-'PY'
		import boto3
		from botocore.config import Config
		s3 = boto3.client("s3", endpoint_url="$(S3_URL)",
		    aws_access_key_id="soma", aws_secret_access_key="soma-secret",
		    region_name="us-east-1",
		    config=Config(s3={"addressing_style": "path"}, signature_version="s3v4"))
		s3.create_bucket(Bucket="smoke")
		s3.put_object(Bucket="smoke", Key="k", Body=b"hello")
		assert s3.get_object(Bucket="smoke", Key="k")["Body"].read() == b"hello"
		s3.delete_object(Bucket="smoke", Key="k")
		print("smoke: create/put/get/delete OK")
	PY

# ── erasure-coded cluster (6 storage, Reed-Solomon 4+2) ────────────────────

ec-up: ## Start a 6-node erasure-coded (k=4 m=2) cluster
	$(EC_COMPOSE) up -d --build
	@printf "\nErasure cluster starting — gateway waits for all 6 storage nodes.\n"
	@printf "Check readiness: make ready    Degraded-read demo: make ec-degraded\n"

ec-down: ## Stop the erasure cluster (KEEPS data volumes)
	$(EC_COMPOSE) down

ec-clean: ## Stop the erasure cluster and DELETE its data volumes
	$(EC_COMPOSE) down -v --remove-orphans

ec-ps: ## Show erasure-cluster container status
	$(EC_COMPOSE) ps

ec-logs: ## Tail erasure-cluster logs
	$(EC_COMPOSE) logs -f --tail=200

ec-degraded: ## Write an object, kill 2 storage nodes, read it back reconstructed, restore
	@python3 - <<-'PY'
		import boto3
		from botocore.config import Config
		s3 = boto3.client("s3", endpoint_url="$(S3_URL)",
		    aws_access_key_id="soma", aws_secret_access_key="soma-secret",
		    region_name="us-east-1",
		    config=Config(s3={"addressing_style": "path"}, signature_version="s3v4"))
		s3.create_bucket(Bucket="ec")
		s3.put_object(Bucket="ec", Key="obj", Body=b"erasure-coded payload " * 1000)
		print("wrote object across all 6 shards")
	PY
	$(EC_COMPOSE) stop storage-4 storage-5
	@python3 - <<-'PY'
		import boto3
		from botocore.config import Config
		s3 = boto3.client("s3", endpoint_url="$(S3_URL)",
		    aws_access_key_id="soma", aws_secret_access_key="soma-secret",
		    region_name="us-east-1",
		    config=Config(s3={"addressing_style": "path"}, signature_version="s3v4"))
		got = s3.get_object(Bucket="ec", Key="obj")["Body"].read()
		assert got == b"erasure-coded payload " * 1000, "reconstruct mismatch"
		print("read OK with 2 of 6 nodes down — reconstructed from 4 survivors")
	PY
	$(EC_COMPOSE) start storage-4 storage-5
