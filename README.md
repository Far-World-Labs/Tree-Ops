# Agentic Storage API

FastAPI service for agentic planning and state management with hierarchical data structures.

## Setup

```bash
# Install dependencies and git hooks
make install

# Copy environment variables
cp .env.example .env

# Start PostgreSQL
docker-compose up -d

# Run migrations
make migrate

# Start development server
make dev
```

## Development

```bash
# Run tests
make test

# Watch tests (auto-rerun on changes)
make test-watch

# Run linters and formatters
make lint     # Check code style
make format   # Auto-format code

# Type checking
uv run pyright

# Create a migration
make migration msg="Description here"
```

## Project Structure

```
app/
├── ops/           # Planning and state APIs
│   ├── entities/  # Database models
│   ├── routes/    # API endpoints
│   ├── services/  # Business logic
│   └── schemas.py # Request/response models
├── lib/           # Shared utilities
│   └── db/        # Database configuration
└── middleware.py  # Request tracing and timing

tests/             # Integration tests
└── factories/     # Test data factories
```
