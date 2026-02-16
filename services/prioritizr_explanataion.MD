# Prioritizr Postgres Runner

This documentation for `Prioritizr_Postgres_V2.R` script.

- What the script does
- Why it is implemented this way
- How it interacts with Postgres, PostGIS, H3, and Prioritizr
- How runs are reproducible, debuggable, and auditable

---

## 1. Purpose of the Script

This script executes a **Prioritizr conservation optimization** using:

- Postgres / PostGIS as the authoritative data store
- H3 indexes for planning units and adjacency
- A server‑side controller (e.g. Tornado) for execution and log streaming
- No filesystem‑based inputs or outputs

The script is designed to be:

- Deterministic
- Reproducible
- Stateless
- Safe for concurrent execution
- Scalable to very large planning unit counts

---

## 2. Invocation Contract

Run script using the following format  
This is done internally by Tornado by the BioProtect Server

```
Rscript Prioritizr_Postgres_V2.R <run_id>
```

### Why only `run_id`?

The `run_id` uniquely identifies **one specific prioritization run**. All configuration, parameters, and inputs are resolved from the database using this identifier.

This design:

- Avoids fragile command‑line argument lists
- Prevents configuration drift between server and R
- Ensures every run is auditable and reproducible
- Keeps the R process stateless and side‑effect free

---

## 3. Database‑Driven Architecture

### Core Principle

**The database is the source of truth.**

The R script does **not**:

- Decide which features are included
- Decide which planning units exist
- Decide which cost profile applies
- Decide which parameters are valid

It only executes a run that has already been fully defined and validated upstream.

---

## 4. End‑to‑End Run Lifecycle

1. A user triggers a run via the UI
2. The server creates a new row in `bioprotect.prioritizr_runs`
3. Postgres prepares spatial inputs via `prepare_prioritizr_input(run_id)`
4. The server launches this script with the `run_id`
5. The script loads configuration, reads data, runs the optimizer, and writes results
6. Logs stream live to the UI via stdout/stderr

Each run is isolated and repeatable.

---

## 5. `prepare_prioritizr_input(run_id)`

Before this script runs, the database executes:

```
bioprotect.prepare_prioritizr_input(run_id)
```

This function:

- Resolves the project and active cost profile
- Aggregates feature amounts per planning unit
- Produces a **per‑run wide input table**

Example table name:

```
bioprotect.prioritizr_input_run_<run_id>
```

### Columns in the Prepared Table

- `pu_id` — H3 index identifying the planning unit
- `geometry` — Planning unit geometry
- `cost` — Cost from the active cost profile
- `area_km2` — Area in square kilometers
- `f_<id>` — Feature amount for feature `<id>`

### Why a Per‑Run Table?

- Eliminates heavy joins in R
- Guarantees consistent inputs across retries
- Allows multiple runs with different parameters
- Makes the R script purely computational

---

## 6. Feature Columns (`feature_cols`)

### What They Represent

`feature_cols` is a list of column names such as:

```
f_19, f_21, f_22, f_37, f_71
```

Each corresponds to a **feature ID**, not a feature name.

### Why Use IDs Instead of Names?

- Names may be long, duplicated, or unsafe in SQL/R
- Numeric IDs guarantee uniqueness
- Mapping back to real features is handled via database tables

This design is intentional and required for robustness.

---

## 7. Loading Run Configuration

The script retrieves configuration using:

```
SELECT * FROM bioprotect.get_prioritizr_run_config(run_id)
```

This returns **resolved, validated values**, not raw user input.

Fields include:

- `project_id`
- `input_table`
- `feature_cols`
- `target_prop`
- `mode`
- `boundary_penalty`
- `linear_cost_penalty`
- `gap`
- `time_limit_sec`

The R script never infers configuration.

---

## 8. Feature Sanitization (Critical Step)

After loading planning units:

- Feature columns are coerced to numeric
- `NA`, `NaN`, and `Inf` values are replaced with `0`
- Features with **zero total coverage** are dropped

### Why This Is Mandatory

Prioritizr fails or produces invalid results when:

- A feature has no coverage
- Feature columns contain non‑numeric values

This step prevents solver crashes and silent logical errors.

---

## 9. Boundary Matrix Construction (H3)

Adjacency is derived using:

```
bioprotect.get_project_h3_adjacency(project_id)
```

### Why H3?

- Deterministic topology
- No geometry intersection cost
- Global‑scale performance
- Consistent neighbor definitions

### Matrix Rules

- Built as a sparse symmetric matrix
- Diagonal populated with row sums
- Automatically skipped for large PU counts
- Converted to a `dsCMatrix` for prioritizr compatibility

---

## 10. Optimization Modes

### Area Mode

- Cost column: `area_km2`
- Objective: minimize shortfall
- Budget = total area × target proportion

### Cost Mode

- Cost column: `cost`
- Objective: minimum set
- Targets applied directly

Both modes use binary decisions and optional penalties.

---

## 11. Solver Selection

The script explicitly uses the CBC solver:

```
add_cbc_solver(...)
```

### Why CBC?

- Open source
- Supported by prioritizr
- Suitable for large binary problems
- No licensing constraints

If no solver is installed, the script fails immediately and loudly.

---

## 12. Logging and Streaming

All progress output uses:

```
cat(...)
flush.console()
```

This allows:

- Real‑time log streaming to the UI
- No filesystem logs
- No buffering delays

This mirrors the behavior of Marxan‑style solvers.

---

## 13. Error Handling Philosophy

The script:

- Fails fast on invalid state
- Does not silently fix logical errors
- Surfaces errors directly to the UI
- Prioritizes correctness over convenience

Errors are signals, not recoverable events.

---

## 14. Result Persistence

Results are written to:

```
bioprotect.prioritizr_run_results
```

Each row stores:

- `run_id`
- `h3_index`
- `solution` (0 or 1)

This enables scenario comparison, visualization, and auditing.

---

## 15. Reproducibility Guarantees

A run is reproducible if:

- The `run_id` exists
- The input table exists or can be regenerated
- Solver versions are consistent

No external files are required.

---

## 16. Design Summary

This script is intentionally:

- Database‑driven
- Stateless
- Deterministic
- Scalable
- Auditable
- Safe for concurrent execution

Every design decision prioritizes correctness, traceability, and debuggability.
