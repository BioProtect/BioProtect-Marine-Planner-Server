# Prioritizr optimizer (Postgres + H3 adjacency)
# Usage: Rscript --vanilla --slave run_prioritzr_v2.R <run_id>
#   (started by handlers/prioritizr_websocket_handler.py)

# Key changes:
#  - argv is just run_id
#  - read config from get_prioritizr_run_config(run_id)
#  - read PUs from input_table using feature_cols
#  - build boundary matrix using get_project_h3_adjacency(project_id)
#  - write results into prioritizr_run_results
#  - Also: use cat(...); flush.console() so Tornado can stream lines.
#changes made 26-03 by Pablo:
#Optional lock-in and lock-out support
#Optional feature-specific relative targets support

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

`%||%` <- function(x, y) {
    if (is.null(x) || length(x) == 0 || all(is.na(x))) y else x
}

# Use HiGHS if available, otherwise fall back to CBC
add_solver <- if (requireNamespace("highs", quietly = TRUE)) {
  add_highs_solver
} else {
  add_cbc_solver
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
PG_DB <- Sys.getenv("PGDATABASE", "bioprotect")
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

db_info <- dbGetQuery(
    conn,
    "
    SELECT
    current_database()  AS db,
    current_user        AS user,
    inet_server_addr()  AS server_ip,
    inet_server_port()  AS server_port
    "
)
logline("DB INFO:")
logline(capture.output(print(db_info)))

sp <- dbGetQuery(conn, "SHOW search_path")
logline("search_path:")
logline(sp$search_path)


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
feature_cols_raw <- config$feature_cols[[1]]

# feature_cols may arrive as:
# - a character vector (ideal), OR
# - a Postgres array literal string like "{f_19,f_21,...}"
feature_cols <- NULL
if (is.character(feature_cols_raw) && length(feature_cols_raw) > 1) {
    feature_cols <- feature_cols_raw
} else if (is.character(feature_cols_raw) && length(feature_cols_raw) == 1) {
    tmp <- gsub("[{}\"]", "", feature_cols_raw)
    tmp <- trimws(tmp)
    feature_cols <- if (nzchar(tmp)) {
        trimws(strsplit(tmp, ",")[[1]])
    } else {
        character(0)
    }
} else {
    feature_cols <- as.character(unlist(feature_cols_raw))
}

feature_cols <- feature_cols[nzchar(feature_cols)]
if (length(feature_cols) == 0) {
    stop("Run has no feature_cols.")
}

if (is.null(input_table) || !nzchar(input_table)) {
    stop("Run has no input_table. Did you call prepare_prioritizr_input?")
}
if (is.null(feature_cols) || length(feature_cols) == 0) {
    stop("Run has no feature_cols.")
}

# ---- 3.5) Detect optional (locked_in/locked_out) columns in input_table ----------
input_parts <- strsplit(input_table, ".", fixed = TRUE)[[1]]
input_schema <- if (length(input_parts) == 2) input_parts[1] else "public"
input_name <- if (length(input_parts) == 2) input_parts[2] else input_parts[1]

available_cols <- dbGetQuery(
    conn,
    sprintf(
        paste(
            "SELECT column_name",
            "FROM information_schema.columns",
            "WHERE table_schema = '%s' AND table_name = '%s'"
        ),
        input_schema,
        input_name
    )
)$column_name

has_locked_in <- "locked_in" %in% available_cols
has_locked_out <- "locked_out" %in% available_cols

# Optimizer params
TARGET_PROP <- as.numeric(config$target_prop[1] %||% 0.30)
# The objective is chosen in 4.7 from the feature amounts and lock-outs:
# min-set, or area (min shortfall) only when lock-outs make a target
# unreachable. config$mode is unused: the app never sends one, and
# get_prioritizr_run_config's 'area' default is ignored.
MODE <- NA_character_
AREA_BUDGET_PROP <- 0.35  # fixed fraction of grid area, used only for shortfall fallback
# Clustering strength 0..1 (the slider; stored as penalties.boundary).
# Runs before 2026-11-11 stored absolute boundary penalties (<= 0.005) here.
CLUSTERING <- min(max(as.numeric(config$boundary_penalty[1] %||% 0), 0), 0.95)
CLUSTER_BUDGET_SEC <- 240 # all solves incl. round 0; inside the 5-minute ceiling
CLUSTER_MAX_ROUNDS <- 15
CLUSTER_NEIGHBOUR_TOL <- 1e-3 # maximum remaining change in neighbour share
AREA_REWARD <- 0.05 # area mode: clustering reward vs a cell's mean target contribution
COHON <- isTRUE(as.logical(config$cohon_calibrate[1]))
LINEAR_COST_PENALTY <- as.numeric(config$linear_cost_penalty[1] %||% 0.1)
GAP <- as.numeric(config$gap[1] %||% 0.04)
TIME_LIMIT_SEC <- as.integer(config$time_limit_sec[1] %||% 1200)

logline("Run:", run_id, " Project:", project_id)
logline("Input table:", input_table)
logline("Features:", length(feature_cols))


# ---- 4) Read PU input table (wide) ----------
# select geometry because st_read expects it; cost + area + feature cols.
# one scan and avoids joins.
# sel <- c("pu_id", "geometry", "cost", "area_km2", feature_cols)
sel <- c("pu_id", "geometry", "cost", "area_km2")
if (has_locked_in) {
    sel <- c(sel, "locked_in")
}
if (has_locked_out) {
    sel <- c(sel, "locked_out")
}
sel <- c(sel, feature_cols)

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
# ---- 4.4) Locked-in / locked-out cleanup ----
if (!("locked_in" %in% names(PU))) {
    PU$locked_in <- FALSE
}
if (!("locked_out" %in% names(PU))) {
    PU$locked_out <- FALSE
}

PU$locked_in <- as.logical(as.integer(PU$locked_in))
PU$locked_out <- as.logical(as.integer(PU$locked_out))

# Prevent impossible PUs (both locked in and locked out)
if (any(PU$locked_in & PU$locked_out, na.rm = TRUE)) {
    stop(
        "Some PUs are both locked_in and locked_out. Fix input_table before running."
    )
}

# -------------------------------
# 4.5) Drop zero-coverage features
# -------------------------------
# use a numeric matrix to avoid colSums complaining
feat_mat <- as.matrix(PU[, feature_cols, drop = FALSE])
storage.mode(feat_mat) <- "double"
feat_sums <- colSums(feat_mat, na.rm = TRUE)

valid_features <- names(feat_sums[is.finite(feat_sums) & feat_sums > 0])
if (length(valid_features) == 0) {
    stop("All features have zero coverage in this project")
}

# Features with no coverage cannot be targeted and are excluded from the
# problem. This used to happen silently, which looks identical to "the
# optimizer ignored my feature" -- log it loudly so the run log says which
# features were dropped and why.
dropped_features <- setdiff(feature_cols, valid_features)
if (length(dropped_features) > 0) {
    logline(
        "WARNING: dropping",
        length(dropped_features),
        "feature(s) with zero coverage in this project's planning units:",
        paste(dropped_features, collapse = ", "),
        "- check that the feature was preprocessed against THIS project's",
        "planning grid and that it overlaps the project extent."
    )
}

feature_cols <- valid_features
logline("Using features:", paste(feature_cols, collapse = ", "))

# ---- 4.6) Per-feature relative targets ----------
feature_targets <- setNames(
    rep(TARGET_PROP, length(feature_cols)),
    feature_cols
)

if ("feature_targets_json" %in% names(config)) {
    raw_targets <- config$feature_targets_json[1]

    if (!is.null(raw_targets) && !is.na(raw_targets) && nzchar(raw_targets)) {
        parsed_targets <- unlist(
            jsonlite::fromJSON(raw_targets),
            use.names = TRUE
        )

        if (length(parsed_targets) > 0) {
            # NB: as.numeric() strips names. Rebuild the named vector via
            # storage.mode() so the f_<id> -> target mapping survives;
            # otherwise the filter below always matched nothing and every
            # feature silently fell back to the default target.
            target_names <- names(parsed_targets)
            parsed_targets <- setNames(
                suppressWarnings(as.numeric(parsed_targets)),
                target_names
            )
            parsed_targets <- parsed_targets[
                !is.na(parsed_targets) & names(parsed_targets) %in% feature_cols
            ]

            if (length(parsed_targets) > 0) {
                feature_targets[names(parsed_targets)] <- parsed_targets
            }
        }
    }
}

logline(
    "Targets:",
    paste(
        sprintf("%s=%.3f", names(feature_targets), feature_targets),
        collapse = " "
    )
)

# ---- 4.7) Select objective using a fast, exact per-feature feasibility check ----
# Minimum-set has no area budget. With only relative targets and PU locks,
# selecting all non-locked-out PUs is a feasible solution exactly when each
# feature has enough remaining amount to reach its target.
locked_out_amount <- colSums(
    feat_mat[which(PU$locked_out), feature_cols, drop = FALSE]
)
available_amount <- feat_sums[feature_cols] - locked_out_amount
required_amount <- feat_sums[feature_cols] * feature_targets[feature_cols]
# a hair of tolerance for float sums; a 100% target with no lock-outs is
# exactly feasible and must stay min-set
MODE <- if (all(available_amount >= required_amount * (1 - 1e-9))) "species" else "area"

logline(
    "Mode:",
    MODE,
    " Target:",
    TARGET_PROP,
    " Gap:",
    GAP,
    " TimeLimit:",
    TIME_LIMIT_SEC,
    " Clustering:",
    CLUSTERING,
    " Cohon:",
    COHON
)

# ---- 5) Neighbour matrix from precomputed edges (or runtime fallback) ----------
# Clustering needs only who-neighbours-whom; edge lengths are uniform on an
# H3 grid. Skipped entirely when clustering is off.
nm <- NULL
if (CLUSTERING > 0) {
    logline("Loading neighbour edges…")
    h3_adjacency <- dbGetQuery(
        conn,
        sprintf(
            "SELECT pu_id, nbr_id FROM bioprotect.get_project_boundary_edges(%s)",
            project_id
        )
    )
    if (nrow(h3_adjacency) == 0) {
        # Fallback: compute at runtime (fine for small grids, slow for 100K+)
        logline("No precomputed edges found, computing from H3 adjacency…")
        h3_adjacency <- dbGetQuery(
            conn,
            sprintf(
                "SELECT pu_id, nbr_id FROM bioprotect.get_project_h3_adjacency(%s)",
                project_id
            )
        )
    }
    i <- match(as.character(h3_adjacency$pu_id), PU$pu_id)
    j <- match(as.character(h3_adjacency$nbr_id), PU$pu_id)
    ok <- !is.na(i) & !is.na(j) & i != j
    if (!any(ok)) {
        logline("No neighbour edges matched the PUs; continuing without clustering.")
    } else {
        n <- nrow(PU)
        nm <- Matrix::sparseMatrix(
            i = c(i[ok], j[ok]), j = c(j[ok], i[ok]), x = 1, dims = c(n, n)
        )
        nm@x[] <- 1 # duplicate edges sum to 2; neighbours are 0/1
        logline("Neighbour matrix built. edges=", sum(ok))
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
    budget <- AREA_BUDGET_PROP * sum(PU$area_km2, na.rm = TRUE)
    # Locked-in cells are always selected; a budget below their area makes
    # the problem infeasible and the run fail.
    locked_in_area <- sum(PU$area_km2[which(PU$locked_in)])
    if (locked_in_area > budget) {
        logline(sprintf(
            "Area budget raised from %.0f to the locked-in area %.0f km2: the plan is the locked-in cells",
            budget, locked_in_area
        ))
        budget <- locked_in_area * (1 + 1e-9) # solver tolerance on the equal sum
    }
    # Scale the cost tie-breaker to the grid. The shortfall objective is at
    # most sum(feature weights), but raw cost grows with the cell count, so a
    # fixed 0.1 x cost outweighed every target on large grids: 20k cells met
    # 0/12 targets, 192k selected nothing (tested 2026-11-11). Now selecting
    # every cell costs LINEAR_COST_PENALTY x the full shortfall.
    total_cost <- sum(PU$cost, na.rm = TRUE)
    cost_penalty <- if (total_cost > 0) LINEAR_COST_PENALTY * sum(fw) / total_cost else 0
}

# `nbr` = each PU's (damped) share of selected neighbours from the previous
# round, 0..1. Round 0 has none, i.e. the plain problem.
build_problem <- function(nbr, time_limit) {
    if (MODE == "area") {
        # Area is the budget, so it can't be discounted; cost is only a weak
        # tie-breaker here. Reward clustered cells on the shortfall scale
        # instead: AREA_REWARD x a cell's mean contribution to the targets.
        p <- problem(PU, features = feature_cols, cost_column = "area_km2") |>
            add_min_shortfall_objective(budget = budget) |>
            add_relative_targets(feature_targets) |>
            add_feature_weights(fw) |>
            add_linear_penalties(penalty = cost_penalty, data = PU$cost)
        if (any(nbr > 0)) {
            p <- p |> add_linear_penalties(
                penalty = -1,
                data = AREA_REWARD * shortfall_unit * CLUSTERING * nbr
            )
        }
    } else {
        d <- PU
        d$cost_r <- PU$cost * (1 - CLUSTERING * nbr)
        p <- problem(d, features = feature_cols, cost_column = "cost_r") |>
            add_min_set_objective() |>
            add_relative_targets(feature_targets) |>
            add_feature_weights(fw)
    }
    p <- p |>
        add_binary_decisions() |>
        add_solver(gap = GAP, time_limit = time_limit, verbose = TRUE)
    if (any(PU$locked_in, na.rm = TRUE)) {
        p <- p |> add_locked_in_constraints(locked_in = "locked_in")
    }
    if (any(PU$locked_out, na.rm = TRUE)) {
        p <- p |> add_locked_out_constraints(locked_out = "locked_out")
    }
    p
}

if (any(PU$locked_in, na.rm = TRUE)) {
    logline("Locked-in PUs:", sum(PU$locked_in, na.rm = TRUE))
}
if (any(PU$locked_out, na.rm = TRUE)) {
    logline("Locked-out PUs:", sum(PU$locked_out, na.rm = TRUE))
}

sol_of <- function(s) {
    v <- as.numeric(s[[if ("solution_1" %in% names(s)) "solution_1" else "solution"]])
    v[!is.finite(v)] <- 0
    v
}

t_start <- Sys.time()
elapsed <- function() as.numeric(difftime(Sys.time(), t_start, units = "secs"))

# Area mode, on request: Cohon's method (prioritizr::calibrate_cohon_penalty)
# picks the cost penalty where cost and target shortfall are traded evenly.
# Here it works out as n_features / cost of the cost-blind plan, about 30x the
# normalised default: cheaper plans, some targets fall short (20k test cells:
# cost -20%, worst feature at 83% of target). Two extra solves; they count
# against the clustering budget. Falls back to the normalised penalty.
if (MODE == "area" && COHON && cost_penalty > 0) {
    logline("Calibrating cost penalty (Cohon)…")
    calibrated <- tryCatch(
        calibrate_cohon_penalty(
            build_problem(rep(0, nrow(PU)), TIME_LIMIT_SEC),
            approx = TRUE, verbose = FALSE
        ),
        error = function(e) {
            logline("Cohon calibration failed, using the normalised penalty:", conditionMessage(e))
            NA
        }
    )
    if (is.finite(calibrated) && calibrated > 0) {
        logline(sprintf(
            "Cost penalty: Cohon %.4g (normalised would be %.4g)", calibrated, cost_penalty
        ))
        cost_penalty <- as.numeric(calibrated)
    }
} else if (MODE == "area") {
    logline(sprintf("Cost penalty: normalised %.4g", cost_penalty))
}

logline("Solving…")
s <- solve(build_problem(rep(0, nrow(PU)), TIME_LIMIT_SEC), force = TRUE)

# Clustering: iterative cost discount (tested 2026-11-11: 192k hexes,
# -79% cut edges at +14% cost in ~200 s). Pairwise boundary/neighbour
# penalties don't finish above ~50k hexes with HiGHS/CBC. Each round is a
# full solve with the same targets and locks, so any round's plan is valid
# and we can stop whenever the time budget runs out.
if (!is.null(nm)) {
    deg <- pmax(Matrix::rowSums(nm), 1)
    amt <- as.matrix(sf::st_drop_geometry(PU)[feature_cols])
    tot <- colSums(amt)
    tot[tot <= 0] <- 1
    shortfall_unit <- mean(rowSums(sweep(amt, 2, tot, "/")))
    e_i <- Matrix::summary(Matrix::triu(nm))
    cut_count <- function(v) sum(v[e_i$i] != v[e_i$j])

    v <- sol_of(s)
    nbr <- rep(0, nrow(PU))
    logline(sprintf("Clustering round 0: %d PUs, %d cut edges", sum(v), cut_count(v)))
    last_round <- elapsed()
    for (k in seq_len(CLUSTER_MAX_ROUNDS)) {
        t_round <- elapsed()
        remaining <- as.integer(floor(CLUSTER_BUDGET_SEC - t_round))
        # stop if another round (about as long as the last) won't fit
        if (remaining < 1L || remaining < last_round) {
            logline("Clustering: time budget reached after", k - 1, "rounds")
            break
        }
        selected_neighbour_share <- as.vector(nm %*% v) / deg
        nbr <- 0.5 * nbr + 0.5 * selected_neighbour_share
        s_k <- tryCatch(
            solve(build_problem(nbr, remaining), force = TRUE),
            error = function(e) {
                logline("Clustering round", k, "failed, keeping round", k - 1, ":", conditionMessage(e))
                NULL
            }
        )
        if (is.null(s_k)) break
        v_k <- sol_of(s_k)
        last_round <- elapsed() - t_round
        logline(sprintf(
            "Clustering round %d: %d PUs, %d cut edges (%.0f s)",
            k, sum(v_k), cut_count(v_k), last_round
        ))
        changed <- any(v_k != v)
        s <- s_k
        v <- v_k
        # With an unchanged selection, this is the discount's remaining drift.
        # Keep iterating while damping still materially changes the costs.
        if (!changed && max(abs(nbr - selected_neighbour_share)) <= CLUSTER_NEIGHBOUR_TOL) {
            logline("Clustering converged")
            break
        }
    }
}

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
