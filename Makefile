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
S3_URL     ?= http://127.0.0.1:9000
ADMIN_URL  ?= http://127.0.0.1:9001
IMAGE      ?= soma:dev

.DEFAULT_GOAL := help

.PHONY: help \
        build test lint fmt fmt-check check run \
        image up down restart rebuild logs ps sh clean \
        ready health metrics smoke

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
