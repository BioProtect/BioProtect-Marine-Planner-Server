# Prioritizr optimizer
# Usage: Rscript Prioritizr_Postgres_V2.R params.json
rm(list = ls())
# ---- install libraries if missing ----
need <- c("sf", "DBI", "RPostgres", "prioritizr", "Matrix", "jsonlite", "rcbc")
# to_install <- setdiff(need, rownames(installed.packages()))
# if (length(to_install)) {
#   install.packages(to_install, repos = "https://cloud.r-project.org")
# }
suppressPackageStartupMessages({
    library(sf)
    library(DBI)
    library(RPostgres)
    library(prioritizr)
    library(Matrix)
    library(jsonlite)
})

`%||%` <- function(x, y) if (is.null(x) || length(x) == 0) y else x

# ---- 1) Read JSON ----------
args <- commandArgs(trailingOnly = TRUE)
if (!length(args)) {
    stop("Pass a JSON file path (or JSON string).")
}
raw <- args[1]
p <- tryCatch(
    {
        if (file.exists(raw)) fromJSON(raw) else fromJSON(raw)
    },
    error = function(e) stop("Could not parse JSON: ", e$message)
)
# Required JSON keys
PU_TABLE <- p$pu$table %||% stop("JSON pu$table is required.")
GEOM_COL <- p$pu$geom_col %||% "geometry"
ID_COL <- p$pu$id_col %||% stop("JSON pu$id_col is required.")
COST_COL <- p$pu$cost_col %||% "cost"
FEAT_COLS <- p$pu$features %||%
    stop("JSON pu$features (array of feature col names) is required.")
AREA_COL <- p$pu$area_col %||% NULL

TARGET_PROP <- p$targets$prop %||% 0.30
MODE <- p$mode %||% "area" # "area" or "species"
BOUNDARY_PENALTY <- p$penalties$boundary %||% 0.005
LINEAR_COST_PENALTY <- p$penalties$linear %||% 0.1
#PORTFOLIO_N         <- p$portfolio$n        %||% 15
MAX_PU_FOR_BOUNDARY <- p$limits$max_pu_bmat %||% 60000
TIME_LIMIT_SEC <- p$solver$time_limit %||% 1200
GAP <- p$solver$gap %||% 0.04
#THREADS             <- as.integer(p$solver$threads %||% 1)  # CBC expects a positive integer

PG <- p$db %||%
    list(
        host = "localhost",
        port = 5432,
        dbname = "postgres",
        user = "postgres",
        password = "postgres"
    )

# ---- 2) Connect & read just the needed columns --------
message("Connecting to Postgres…")
conn <- dbConnect(
    Postgres(),
    host = PG$host,
    port = PG$port,
    dbname = PG$dbname,
    user = PG$user,
    password = PG$password
)
on.exit(try(dbDisconnect(conn), silent = TRUE), add = TRUE)

parts <- strsplit(PU_TABLE, "\\.", fixed = FALSE)[[1]]
qtbl <- paste(DBI::dbQuoteIdentifier(conn, parts), collapse = ".")
tbl_cols <- names(DBI::dbGetQuery(
    conn,
    paste0("SELECT * FROM ", qtbl, " LIMIT 0")
))
must_have <- unique(c(
    ID_COL,
    GEOM_COL,
    COST_COL,
    FEAT_COLS,
    if (!is.null(AREA_COL)) AREA_COL
))
miss <- setdiff(must_have, tbl_cols)
if (length(miss)) {
    stop("Missing columns in PU table: ", paste(miss, collapse = ", "))
}
sel_parts <- c(
    sprintf("%s AS pu_id", DBI::dbQuoteIdentifier(conn, ID_COL)),
    paste0(DBI::dbQuoteIdentifier(conn, GEOM_COL), " AS geometry"),
    sprintf("%s AS cost", DBI::dbQuoteIdentifier(conn, COST_COL)),
    DBI::dbQuoteIdentifier(conn, FEAT_COLS)
)
if (!is.null(AREA_COL)) {
    sel_parts <- c(
        sel_parts,
        paste0(DBI::dbQuoteIdentifier(conn, AREA_COL), " AS area_km2")
    )
}
sel_sql <- paste(sel_parts, collapse = ", ")

qry <- sprintf("SELECT %s FROM %s", sel_sql, qtbl)
message("Reading PUs: ", PU_TABLE)
PU <- tryCatch(
    sf::st_read(conn, query = qry, quiet = TRUE),
    error = function(e) stop("Failed to read PU table: ", e$message)
)
# ---- 3) Light cleanup ------------------------------------------------------
# if (is.na(sf::st_crs(PU))) warning("PU geometry has no CRS; proceeding as-is.")
# PU <- sf::st_make_valid(PU)
feat_cols <- FEAT_COLS
for (f in feat_cols) {
    PU[[f]] <- suppressWarnings(as.numeric(PU[[f]]))
    PU[[f]][!is.finite(PU[[f]])] <- 0
}
# Cost fallback to 1 if missing/NA
if (!"cost" %in% names(PU)) {
    PU$cost <- NA_real_
}
if (all(is.na(PU$cost))) {
    message("No valid 'cost' values; using cost = 1.")
    PU$cost <- 1
}
PU$cost[!is.finite(PU$cost)] <- 1

# ---- 4) Boundary matrix ----------------------------------------
add_boundary <- nrow(PU) <= MAX_PU_FOR_BOUNDARY
bm <- NULL
if (add_boundary) {
    message("Building boundary matrix …")
    bm <- try(prioritizr::boundary_matrix(PU), silent = TRUE)
    if (inherits(bm, "try-error")) {
        warning(
            "boundary_matrix failed; continuing without boundary penalties."
        )
        bm <- NULL
    }
} else {
    message(
        "PU count (",
        nrow(PU),
        ") > ",
        MAX_PU_FOR_BOUNDARY,
        " — skipping boundary matrix."
    )
}

# ---- 5) Build & solve problem ---------------------------------------------
fw <- setNames(rep(1, length(feat_cols)), feat_cols)

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
    pblm <- problem(PU, features = feat_cols, cost_column = "area_km2") |>
        add_min_shortfall_objective(budget = budget) |>
        add_relative_targets(TARGET_PROP) |>
        add_feature_weights(fw) |>
        add_linear_penalties(penalty = LINEAR_COST_PENALTY, data = PU$cost) |>
        add_binary_decisions() |>
        #add_cuts_portfolio(number_solutions = PORTFOLIO_N) |>
        add_cbc_solver(gap = GAP, time_limit = TIME_LIMIT_SEC, verbose = TRUE)
} else {
    pblm <- problem(PU, features = feat_cols, cost_column = "cost") |>
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
message("Solving …")
s <- solve(pblm, force = TRUE)

# ---- 6) Export ID and the solution ---------------------------------------
sol_col <- if ("solution_1" %in% names(s)) {
    "solution_1"
} else if ("solution" %in% names(s)) {
    "solution"
} else {
    stop("No solution column found (expected 'solution_1' or 'solution').")
}

id_col <- "pu_id" # Pick the ID column
# Coerce solution to integers
sol_vec <- as.integer(s[[sol_col]])
sol_vec[!is.finite(sol_vec)] <- 0L

out <- data.frame(id = s[[id_col]], sol = sol_vec, check.names = FALSE)
names(out) <- c(id_col, sol_col)
# ---- 7) Write CSV ----------------------------------------------------------
dir.create("outputs", showWarnings = FALSE, recursive = TRUE)
utils::write.csv(
    out,
    file.path("outputs", "solution_prioritizr.csv"),
    row.names = FALSE
)
message(
    "\nDone. Wrote outputs/solution_prioritizr.csv with columns: ",
    id_col,
    ", ",
    sol_col
)
