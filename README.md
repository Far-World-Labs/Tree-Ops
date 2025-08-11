# Tree Operations Service

High-performance hierarchical tree storage API built with FastAPI and PostgreSQL, optimized for deep trees and large-scale operations.

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
├── ops/           # Tree operations APIs
│   ├── entities/  # Database models (TreeNode)
│   ├── routes/    # API endpoints (tree, stats)
│   ├── services/  # Business logic (TreeService)
│   ├── perf/      # Performance testing utilities
│   ├── stats/     # Metrics collection
│   └── schemas.py # Request/response models
├── lib/           # Shared utilities
│   └── db/        # Database configuration
└── middleware.py  # Request tracing and timing

tests/             # Integration tests
scripts/           # Utility scripts
└── tree-perf      # Performance test runner
```

## Performance Testing

See [app/ops/README.md](app/ops/README.md#performance-testing) for detailed performance test results and analysis.
