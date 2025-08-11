# Tree Operations Service

High-performance hierarchical tree storage API built with FastAPI and PostgreSQL, optimized for deep trees and large-scale operations.

## Key Features

```
Deep Trees:     50,000+ node depth supported
Wide Trees:     500,000+ siblings per parent
Performance:    250+ RPS for 5k node trees
JSON Native:    Direct PostgreSQL JSON generation
Zero Errors:    Robust handling of all edge cases
```

## Quick Start

### Installation
```bash
make install          # Install dependencies and git hooks
cp .env.example .env  # Configure environment
```

### Database Setup
```bash
docker-compose up -d  # Start PostgreSQL
make migrate          # Run migrations
```

### Run Server
```bash
make dev              # Start development server on :8000
```

## Development Commands

### Testing
```bash
make test        # Run test suite
make test-watch  # Auto-rerun on changes
```

### Code Quality
```bash
make lint        # Check code style
make format      # Auto-format code
uv run pyright   # Type checking
```

### Database
```bash
make migration msg="Description"  # Create new migration
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

## Performance Benchmarks

### Run Performance Tests
```bash
DATABASE_URL=postgresql+asyncpg://postgres:postgres@localhost:5430/treeops uv run uvicorn app.main:app
uv run scripts/tree-perf
```

### Performance Results (5000 Node Trees)

```
┏━━━━━━━━━━━━━━━━━┳━━━━━━━━┳━━━━━━━┳━━━━━━━┳━━━━━━━┳━━━━━━━┳━━━━━━┳━━━━━━┳━━━━━━┳━━━━━━━━━┳━━━━━━┳━━━━━┳━━━━━━┓
┃ Scenario        ┃  Nodes ┃ Depth ┃ Trees ┃  R:W  ┃   RPS ┃  P50 ┃  P95 ┃  P99 ┃ ms/node ┃  IDX ┃ SEQ ┃ CPU% ┃
┡━━━━━━━━━━━━━━━━━╇━━━━━━━━╇━━━━━━━╇━━━━━━━╇━━━━━━━╇━━━━━━━╇━━━━━━╇━━━━━━╇━━━━━━╇━━━━━━━━━╇━━━━━━╇━━━━━╇━━━━━━┩
│ deep_100        │    100 │   100 │     1 │ 90:10 │ 235.6 │   30 │  111 │  194 │    1.11 │ 2411 │   1 │    2 │
│ deep_1k         │   1000 │  1000 │     1 │ 90:10 │ 105.1 │   96 │  138 │  182 │    0.14 │ 1257 │   0 │    1 │
│ deep_5k         │   5000 │  5000 │     1 │ 90:10 │  16.3 │  638 │  907 │  928 │    0.18 │  174 │   0 │    3 │
│ deep_10k        │  10000 │ 10000 │     1 │ 90:10 │   7.2 │ 1433 │ 1803 │ 1994 │    0.18 │   92 │   0 │    4 │
│ deep_20k        │  20000 │ 20000 │     1 │ 90:10 │   3.7 │ 2325 │ 3334 │ 3518 │    0.17 │   52 │   0 │    2 │
│ deep_50k        │  50000 │ 50000 │     1 │ 90:10 │   1.5 │ 5449 │ 7574 │ 8059 │    0.15 │   16 │  14 │    2 │
├─────────────────┼────────┼───────┼───────┼───────┼───────┼──────┼──────┼──────┼─────────┼──────┼─────┼──────┤
│ wide_1k         │   1000 │     2 │     1 │ 90:10 │ 171.3 │   93 │  255 │  347 │    0.25 │ 1725 │   0 │    1 │
│ wide_5k         │   5000 │     2 │     1 │ 90:10 │ 128.3 │  133 │  290 │  360 │    0.06 │ 4303 │   0 │    1 │
│ wide_10k        │  10000 │     2 │     1 │ 90:10 │ 102.8 │  173 │  313 │  376 │    0.03 │ 5019 │   0 │    2 │
│ wide_50k        │  50000 │     2 │     1 │ 90:10 │  47.0 │  408 │  671 │  742 │    0.01 │ 4476 │   0 │    3 │
│ wide_100k       │ 100000 │     2 │     1 │ 90:10 │  25.7 │  758 │ 1011 │ 1153 │    0.01 │ 3155 │ 131 │    2 │
│ wide_500k       │ 500000 │     2 │     1 │ 90:10 │   4.8 │ 3559 │ 5871 │ 6141 │    0.01 │ 5044 │  34 │    3 │
├─────────────────┼────────┼───────┼───────┼───────┼───────┼──────┼──────┼──────┼─────────┼──────┼─────┼──────┤
│ forest_single   │   5000 │     8 │     1 │ 90:10 │ 121.6 │  140 │  289 │  376 │    0.06 │ 3197 │   0 │    4 │
│ forest_10       │   5000 │     8 │    10 │ 90:10 │ 118.6 │  141 │  293 │  401 │    0.06 │ 3189 │   0 │    2 │
│ forest_50       │   5000 │     8 │    50 │ 90:10 │ 123.5 │  137 │  283 │  350 │    0.06 │ 4186 │   0 │    1 │
│ forest_100      │   5000 │     8 │   100 │ 90:10 │ 123.4 │  139 │  289 │  358 │    0.06 │ 5137 │   0 │    2 │
│ forest_200      │   5000 │     8 │   200 │ 90:10 │ 130.7 │  126 │  301 │  373 │    0.06 │ 2265 │   0 │    2 │
├─────────────────┼────────┼───────┼───────┼───────┼───────┼──────┼──────┼──────┼─────────┼──────┼─────┼──────┤
│ read_only       │   5000 │     8 │     1 │ 100:0 │ 129.4 │  132 │  273 │  305 │    0.05 │ 2255 │   0 │    2 │
│ mostly_read     │   5000 │     8 │     1 │ 90:10 │ 127.5 │  134 │  299 │  344 │    0.06 │ 2247 │   0 │    2 │
│ balanced_rw     │   5000 │     8 │     1 │ 50:50 │ 120.6 │  140 │  328 │  362 │    0.07 │ 1185 │   0 │    2 │
│ mostly_write    │   5000 │     8 │     1 │ 10:90 │ 142.1 │  113 │  305 │  380 │    0.06 │ 1401 │   0 │    1 │
│ write_only      │   5000 │     8 │     1 │ 0:100 │ 142.2 │  106 │  315 │  420 │    0.06 │ 5365 │   0 │    2 │
├─────────────────┼────────┼───────┼───────┼───────┼───────┼──────┼──────┼──────┼─────────┼──────┼─────┼──────┤
│ insert_simple   │   5000 │     8 │     1 │ 50:50 │ 134.0 │  127 │  303 │  338 │    0.06 │ 2339 │   0 │    2 │
│ insert_deep     │   5000 │     8 │     1 │ 50:50 │ 125.1 │  125 │  318 │  388 │    0.06 │ 3220 │   0 │    2 │
│ insert_mixed    │   5000 │     8 │     1 │ 50:50 │ 119.0 │  128 │  329 │  412 │    0.07 │ 1612 │   0 │    4 │
├─────────────────┼────────┼───────┼───────┼───────┼───────┼──────┼──────┼──────┼─────────┼──────┼─────┼──────┤
│ low_contention  │   5000 │     8 │     1 │ 50:50 │ 251.8 │   19 │   27 │   67 │    0.01 │ 2370 │   0 │    2 │
│ med_contention  │   5000 │     8 │     1 │ 50:50 │ 143.0 │  112 │  315 │  387 │    0.06 │ 9053 │   0 │    1 │
│ high_contention │   5000 │     8 │     1 │ 50:50 │ 218.5 │  186 │  453 │  607 │    0.09 │ 3170 │   0 │    1 │
└─────────────────┴────────┴───────┴───────┴───────┴───────┴──────┴──────┴──────┴─────────┴──────┴─────┴──────┘
```

### Performance Scaling

#### Deep Tree Processing
![Deep Tree RPS vs Node Count](docs/rps_vs_nodes_deep.png)
![Deep Tree Latency vs Node Count](docs/latency_vs_nodes_deep.png)

#### Wide Tree Processing
![Wide Tree RPS vs Node Count](docs/rps_vs_nodes_wide.png)
![Wide Tree Latency vs Node Count](docs/latency_vs_nodes_wide.png)

## API Documentation

See [app/ops/README.md](app/ops/README.md) for detailed API design and architecture documentation.
