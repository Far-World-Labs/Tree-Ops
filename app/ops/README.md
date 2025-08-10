# Tree Operations Service

## Core Design Tension

The fundamental challenge in tree storage systems is that a change to any leaf has implications on the consistency and availability of the entire forest. Our API supports two very different operations:
- Precision leaf-level modifications (POST /api/tree for individual node insertion)
- Whole forest retrieval (GET /api/tree returning all trees with full structure)

This service resolves this tension by opting for full consistency and acceptable availability/latency for moderately-sized trees and forests, with flexibility of operations prioritized over extreme scale.

## What We Optimize For

### Bulk Tree Retrieval with Minimal Application Overhead
- Zero application-level serialization/deserialization: Trees are materialized as JSON entirely in PostgreSQL and passed through as text
- Reduced application memory impact:
  - CPU load shifted to PostgreSQL for JSON construction
  - Application memory only holds the final JSON string, not object graphs
  - No Python object construction or Pydantic serialization overhead
- Single round-trip: The GET /api/tree endpoint returns all trees in one database call using recursive PL/pgSQL functions

### Rapid Append-Style Insertion
- Optimized for append-only child accumulation using gap-based positioning (pos field with 1000 increments)
- Deferred validation: Future enhancement will add a query parameter to POST endpoint for optional parent validation in transaction
- Periodic cleanup/GC: Instead of locking for validation, we'll do periodic cleanup every N insertions for orphaned nodes
- Non-blocking inserts: Nodes without parents can be inserted immediately, avoiding transaction locks

### Ordered Child Nodes
- Deterministic child ordering: Children are always returned sorted by pos field, then by id
- Stable positioning: Gap-based approach (similar to lexorank) maintains order without rebalancing
- Append-friendly: Large gaps (1000) between positions favor append operations

### Horizontal Scalability
- Our approach is naturally amenable to horizontal sharding on org_key
- Each organization's trees are completely independent, enabling clean partitioning
- While composing these JSON graphs is a burden on the database, we have multiple approaches to optimize:
  - Read-around caching per tree
  - Materialized views for stable trees
  - Read replicas for forest queries
  - Shard distribution across PostgreSQL instances

### Future Read-Around Caching
The most significant optimization under consideration is read-around caching for hot trees. This would:
- Cache entire trees (identified by root_id) that are frequently accessed
- Serve actively changing trees from Redis while they're being modified
- Return stable trees from cache, fresh trees from database
- Our model makes it easy to filter and cache at the root node level
- Dramatically reduce PostgreSQL load for read-heavy workloads with localized writes

## Alternative Architectures Considered

### Bucket-Based Tree Storage
We considered storing whole trees as JSON blobs in buckets with parallel retrieval and in-memory find/insert operations:
- Advantages:
  - Extremely fast reads (simple blob retrieval)
  - Very IO-bound with minimal CPU/memory for unchanging forests
  - Simple caching strategy
- Disadvantages:
  - Higher load on insert-time search (must deserialize entire tree)
  - Low operational flexibility (no partial updates)
  - Difficult to maintain consistency across concurrent modifications

### Why Relational Won
The relational approach provides:
- Operational flexibility: Can query, update, and analyze at any granularity
- Future materialization options: Can still implement whole-tree/forest caching
- Standard tooling: Leverages PostgreSQL's mature ecosystem
- Incremental optimization: Can add materialized views, partitioning, etc. as needed

## Resource Utilization Patterns

### CPU Load Distribution
Higher in PostgreSQL:
- Tree traversal during JSON construction (recursive function calls)
- JSON aggregation and ordering operations
- Index maintenance on inserts

Lower in Application:
- No object graph construction
- No recursive Python functions
- Simple pass-through of JSON text

### Memory Usage Patterns
- PostgreSQL: Temporary memory for recursive CTE execution and JSON building
- Application: Minimal - only holds final JSON string
- Network: Full tree structure transferred as compact JSON

## What We DON'T Optimize For

### Tree Structure Operations
- Path-based insertion: No support for inserting nodes with paths that may or may not exist. Design tension between denormalized paths vs parent_id+label uniqueness both have tradeoffs. PostgreSQL's ltree extension could enable path queries like `path ~ 'system.backup.*'` but adds complexity
- Arbitrary reordering: Children can't be efficiently reordered after insertion
- Tree rebalancing: No automatic rebalancing for performance optimization
- Subtree operations: No bulk subtree retrieval, insertion, or overwriting
- Frontier queries: No efficient retrieval of all nodes at level K
- Tree pruning: No built-in pruning operations
- Tree forking: No support for copying/forking trees or subtrees

### Data Management
- Paging/streaming: No support for paginated or streaming tree retrieval (yet)
- Transactional batch inserts: Each insert is independent, no multi-node transactions
- Materialized views: No cached/materialized forest query results (beyond the planned read-around cache)
- Bulk import: Future work needed (bulk export already exists)
- Block packing: No optimization for packing unchanged subtrees into efficient storage blocks
- History tracking: No change history or audit log for tree modifications

### Advanced Query Patterns
- Path-based IDs: Using numeric IDs instead of path-based identifiers limits flexible queries (like CSS selectors)
- Value search: No search within node labels or properties
- Node clustering: No spatial or semantic clustering of nodes
- Graph algorithms: No PageRank or similar algorithms. Trees only, not general graphs. Our edges are non-typed and non-weighted, focusing on storage-style use cases in agentic systems rather than flexible data modeling
- Rollup calculations: No aggregation or rollup computations over subtrees/forests

### Concurrency & Consistency
- Multiple writers per parent: No support for concurrent child insertions under same parent
  - No offset-based insertion with dynamic offset updates
  - No node splitting for concurrent access
  - No tombstone deletion with vector timestamps
  - No CRDT-style undo/redo operations
- Optimistic concurrency: Relies on FastAPI's single-threaded async model
- Conflict resolution: No automatic conflict resolution for concurrent modifications

### Domain Modeling
- No foreign keys: Tree nodes have no relationships to other tables
- No business logic: Nodes are pure data structures without domain-specific operations
- No joins: Cannot efficiently join tree data with business entities
- No node types: All nodes are homogeneous, no type-specific behavior or validation

## Technical Design Decisions

### Why PL/pgSQL Functions?
PostgreSQL's recursive CTEs cannot use aggregate functions (like jsonb_agg) in the recursive term. This fundamental limitation prevents building nested JSON in a single recursive query. Our solution uses recursive PL/pgSQL functions that:
- Handle arbitrary tree depth
- Build JSON entirely in PostgreSQL
- Return properly nested structures
- Maintain clean, readable code

### Schema Design
- Adjacency list with denormalization: Each node stores its root_id for O(1) tree identification
- Gap-based positioning: Using large increments (1000) between positions to minimize reordering
- Composite indexes: Optimized for common query patterns (root_id + parent_id + pos + id)

### Trade-offs
- Read optimization over write flexibility: Fast retrieval but limited modification capabilities
- Simplicity over features: No complex operations to maintain performance guarantees
- Database-centric logic: Leveraging PostgreSQL's JSON capabilities vs application-level processing
- Static structure over dynamic: Optimized for stable trees with append-heavy patterns

## Future Considerations

- Performance characterization: Determine capacities and performance limitations of this implementation
- Streaming large forests: Implement cursor-based pagination for massive trees
- Materialized forest views: Cache frequently accessed tree structures
- Path-based operations: Consider ltree extension for path queries if needed
- Bulk import: Add efficient bulk import to complement existing export
- Versioning: Implement tree versioning for history tracking if required

## Performance Characteristics

- GET /api/tree: O(n) where n is total nodes, single database round-trip
- POST /api/tree: O(1) for append operations, O(log n) for position lookup
- Memory usage: Entire forest loaded into memory for serialization
- Network overhead: Minimal due to direct JSON text response
