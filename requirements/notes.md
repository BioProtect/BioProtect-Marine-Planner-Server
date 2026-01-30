need to install a solver for R
`sudo apt-get install coinor-libcbc-dev coinor-libclp-dev`

need to install R dependencies
`Rscript -e 'install.packages(c("sf","prioritizr","Matrix","jsonlite", "RPostgres", "DBI"), repos="https://cloud.r-project.org")'`

`R -e 'if (!require(remotes)) install.packages("remotes"); remotes::install_github("dirkschumacher/rcbc")'`
