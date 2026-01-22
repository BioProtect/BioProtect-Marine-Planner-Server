# Prioritizr optimizer (Postgres + H3 adjacency)
# Usage: Rscript Prioritizr_Postgres_V2.R params.json
# Usage: Rscript Prioritizr_Postgres_V2.R run_id (params.json is replaced by run_id)

# Key changes:
#  - argv is just run_id
#  - read config from get_prioritizr_run_config(run_id)
#  - read PUs from input_table using feature_cols
#  - build boundary matrix using get_project_h3_adjacency(project_id)
#  - write results into prioritizr_run_results
#  - Also: use cat(...); flush.console() so Tornado can stream lines.

rm(list = ls())

suppressPackageStartupMessages({
    library(sf)
    library(DBI)
    library(RPostgres)
    library(prioritizr)
    library(Matrix)
    library(jsonlite)
})

logline <- function(...) {
    cat(..., "\n")
    flush.console()
}

`%||NA%` <- function(x, y) {
    if (is.null(x) || is.na(x)) y else x
}


# ---- 1) Read run_id ----------
args <- commandArgs(trailingOnly = TRUE)
if (length(args) < 1) {
    stop("Usage: Rscript Prioritizr_Postgres_RunId.R <run_id>")
}
run_id <- suppressWarnings(as.integer(args[1]))
if (!is.finite(run_id) || run_id <= 0) {
    stop("Invalid run_id")
}

# ---- 2) DB connect (use env vars or defaults) ----------
PG_HOST <- Sys.getenv("PGHOST", "localhost")
PG_PORT <- as.integer(Sys.getenv("PGPORT", "5432"))
PG_DB <- Sys.getenv("PGDATABASE", "postgres")
PG_USER <- Sys.getenv("PGUSER", "postgres")
PG_PASS <- Sys.getenv("PGPASSWORD", "postgres")

logline("Connecting to Postgres…")
conn <- dbConnect(
    Postgres(),
    host = PG_HOST,
    port = PG_PORT,
    dbname = PG_DB,
    user = PG_USER,
    password = PG_PASS
)
on.exit(try(dbDisconnect(conn), silent = TRUE), add = TRUE)

# ---- 3) Load run config ----------
config <- dbGetQuery(
    conn,
    sprintf(
        "SELECT * FROM bioprotect.get_prioritizr_run_config(%s)",
        run_id
    )
)
if (nrow(config) != 1) {
    stop("Run config not found for run_id=", run_id)
}

project_id <- as.integer(config$project_id[1])
input_table <- config$input_table[1]
feature_cols <- config$feature_cols[[1]]

if (is.null(input_table) || !nzchar(input_table)) {
    stop("Run has no input_table. Did you call prepare_prioritizr_input?")
}
if (is.null(feature_cols) || length(feature_cols) == 0) {
    stop("Run has no feature_cols.")
}

# Optimizer params
TARGET_PROP <- as.numeric(config$target_prop[1] %||NA% 0.30)
MODE <- as.character(config$mode[1] %||NA% "area")
BOUNDARY_PENALTY <- as.numeric(config$boundary_penalty[1] %||NA% 0.005)
LINEAR_COST_PENALTY <- as.numeric(config$linear_cost_penalty[1] %||NA% 0.1)
GAP <- as.numeric(config$gap[1] %||NA% 0.04)
TIME_LIMIT_SEC <- as.integer(config$time_limit_sec[1] %||NA% 1200)


logline("Run:", run_id, " Project:", project_id)
logline("Input table:", input_table)
logline("Features:", length(feature_cols))
logline(
    "Mode:",
    MODE,
    " Target:",
    TARGET_PROP,
    " Gap:",
    GAP,
    " TimeLimit:",
    TIME_LIMIT_SEC
)


# ---- 4) Read PU input table (wide) ----------
# select geometry because st_read expects it; cost + area + feature cols.
# one scan and avoids joins.
sel <- c("pu_id", "geometry", "cost", "area_km2", feature_cols)
qry <- paste0("SELECT ", paste(sel, collapse = ", "), " FROM ", input_table)

logline("Reading PUs from prepared input table…")
PU <- tryCatch(
    sf::st_read(conn, query = qry, quiet = TRUE),
    error = function(e) stop("Failed to read prepared PU input: ", e$message)
)

PU$pu_id <- as.character(PU$pu_id)

# Sanitize feature columns -> numeric, NAs -> 0
for (f in feature_cols) {
    PU[[f]] <- suppressWarnings(as.numeric(PU[[f]]))
    PU[[f]][!is.finite(PU[[f]])] <- 0
}

# Cost fallback
PU$cost <- suppressWarnings(as.numeric(PU$cost))
PU$cost[!is.finite(PU$cost)] <- 1

PU$area_km2 <- suppressWarnings(as.numeric(PU$area_km2))
if (any(!is.finite(PU$area_km2) | PU$area_km2 <= 0)) {
    bad <- sum(!is.finite(PU$area_km2) | PU$area_km2 <= 0)
    stop(
        "Found ",
        bad,
        " planning units with non-finite or non-positive area_km2"
    )
}

logline("Loaded PUs:", nrow(PU))

# ---- 5) Boundary matrix from H3 adjacency (Postgres) ----------
# Note: adjacency function returns undirected unique pairs (pu_id, nbr_id).
# mirror edges to make bm symmetric.
logline("Building boundary from H3 adjacency…")
h3_adjacency <- tryCatch(
    dbGetQuery(
        conn,
        sprintf(
            "SELECT pu_id, nbr_id, boundary FROM bioprotect.get_project_h3_adjacency(%s)",
            project_id
        )
    ),
    error = function(e) stop("Failed to read H3 adjacency: ", e$message)
)

bm <- NULL
if (nrow(h3_adjacency) == 0) {
    logline(
        "No adjacency edges returned; continuing without boundary penalties."
    )
} else {
    ids <- PU$pu_id
    n <- length(ids)

    h3_adjacency$pu_id <- as.character(h3_adjacency$pu_id)
    h3_adjacency$nbr_id <- as.character(h3_adjacency$nbr_id)

    i <- match(h3_adjacency$pu_id, ids)
    j <- match(h3_adjacency$nbr_id, ids)
    ok <- !is.na(i) & !is.na(j) & i != j

    if (!any(ok)) {
        logline(
            "Adjacency edges did not match PU ids; continuing without boundary penalties."
        )
    } else {
        x <- suppressWarnings(as.numeric(h3_adjacency$boundary[ok]))
        x[!is.finite(x)] <- 1

        bm <- Matrix::sparseMatrix(
            i = c(i[ok], j[ok]),
            j = c(j[ok], i[ok]),
            x = c(x, x),
            dims = c(n, n),
            dimnames = list(ids, ids)
        )
        logline("Boundary matrix built. nnz=", length(bm@x))
    }
}

# ---- 6) Build & solve problem ----------
fw <- setNames(rep(1, length(feature_cols)), feature_cols)

if (MODE == "area") {
    if (!"area_km2" %in% names(PU)) {
        PU$area_km2 <- rep(1, nrow(PU))
    }
    if (any(!is.finite(PU$area_km2) | PU$area_km2 <= 0)) {
        bad <- sum(!is.finite(PU$area_km2) | PU$area_km2 <= 0)
        stop(
            "Found ",
            bad,
            " planning units with non-finite or non-positive area_km2 values"
        )
    }

    budget <- TARGET_PROP * sum(PU$area_km2, na.rm = TRUE)

    pblm <- problem(PU, features = feature_cols, cost_column = "area_km2") |>
        add_min_shortfall_objective(budget = budget) |>
        add_relative_targets(TARGET_PROP) |>
        add_feature_weights(fw) |>
        add_linear_penalties(penalty = LINEAR_COST_PENALTY, data = PU$cost) |>
        add_binary_decisions() |>
        add_cbc_solver(gap = GAP, time_limit = TIME_LIMIT_SEC, verbose = TRUE)
} else {
    pblm <- problem(PU, features = feature_cols, cost_column = "cost") |>
        add_min_set_objective() |>
        add_relative_targets(TARGET_PROP) |>
        add_feature_weights(fw) |>
        add_binary_decisions() |>
        add_cbc_solver(gap = GAP, time_limit = TIME_LIMIT_SEC, verbose = TRUE)
}

if (!is.null(bm)) {
    pblm <- pblm |>
        add_boundary_penalties(penalty = BOUNDARY_PENALTY, data = bm)
}

logline("Solving…")
s <- solve(pblm, force = TRUE)

# ---- 7) Export ID and the solution ----------
sol_col <- if ("solution_1" %in% names(s)) {
    "solution_1"
} else if ("solution" %in% names(s)) {
    "solution"
} else {
    stop("No solution column found (expected 'solution_1' or 'solution').")
}

sol_vec <- as.integer(s[[sol_col]])
sol_vec[!is.finite(sol_vec)] <- 0L
out <- data.frame(
    h3_index = as.character(s$pu_id),
    solution = sol_vec,
    stringsAsFactors = FALSE
)

logline("Writing results to DB…")
dbBegin(conn)
tryCatch(
    {
        dbWriteTable(
            conn,
            name = Id(schema = "bioprotect", table = "prioritizr_run_results"),
            value = transform(out, run_id = run_id),
            append = TRUE,
            row.names = FALSE
        )

        dbCommit(conn)
    },
    error = function(e) {
        dbRollback(conn)
        stop("Failed writing results: ", e$message)
    }
)

logline("Done. Results rows:", nrow(out))
