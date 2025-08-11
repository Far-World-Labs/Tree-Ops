include .env
export

.PHONY: dev test migrate

# Run the development server
dev:
	uv run uvicorn app.main:app --reload --port 8000

# Run tests
test:
	uv run pytest tests/ -v

# Run tests in watch mode
test-watch:
	uv run watchfiles "uv run pytest tests/ -v" .

# Run database migrations
migrate:
	uv run alembic upgrade head

# Create a new migration
migration:
	uv run alembic revision --autogenerate -m "$(msg)"

# Install dependencies and git hooks
install:
	uv sync
	uv run pre-commit install

# Run linters
lint:
	uv run ruff check .
	uv run pyright .

# Format code
format:
	uv run ruff format .

# Run performance tests
perf:
	uv run python scripts/tree-perf
