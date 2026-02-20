# BioProtect Server

# R Dependencies

```
sudo apt update
sudo apt install -y \
    r-base \
    libpq-dev \
    libssl-dev \
    libcurl4-openssl-dev \
    libxml2-dev \
    libudunits2-dev \
    libgdal-dev \
    libgeos-dev \
    libproj-dev \
    coinor-libcbc-dev \
    cmake \
    g++ \
    make \
    pkg-config
```

```
install.packages(c("sf","DBI","RPostgres","prioritizr","Matrix","jsonlite","rcbc"),
                 repos="https://cloud.r-project.org")
```

**prioritizr automatically detects and uses rcbc if installed. You may need to install gurobi or rsymphony if you want alternative solvers — but rcbc is fully supported.**

`sudo apt update &&  sudo apt install -y coinor-libcbc-dev coinor-libcbc3 coinor-cbc`

rcbc needs installing from git see here - https://github.com/dirkschumacher/rcbc
