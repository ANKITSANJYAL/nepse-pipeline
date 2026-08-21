.PHONY: help install test status run floorsheet eod backfill gold check
help:
	@grep -E '^[a-z-]+:.*?## ' $(MAKEFILE_LIST) | awk -F':.*## ' '{printf "  %-12s %s\n", $$1, $$2}'

install:  ## create venv and install deps
	python3 -m venv .venv && .venv/bin/pip install -q -U pip && .venv/bin/pip install -q -e .

test:     ## run parser/transform tests (no network)
	PYTHONPATH=src .venv/bin/pytest tests/ -q

status:   ## market + warehouse state
	./nepse status

run:      ## live poll loop (foreground)
	./nepse run

floorsheet: ## one full floorsheet sweep + quality gate + silver
	./nepse floorsheet --full --finalize

eod:      ## today's EOD board + gate + silver
	./nepse eod --finalize

backfill: ## walk back through NEPSE's 365-day window (resumable)
	./nepse backfill

gold:     ## build bars / broker flow / daily stats for today
	./nepse gold

daemon:   ## install launchd agent
	./deploy/install.sh
