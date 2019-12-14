#########################################################################
# DESCRIPTION:	Repository for reading/writing to database
# AUTHOR:  Michael Choie
#########################################################################
library(RMySQL)
library(uuid)
library(dplyr)

# Connect to docker database
host_env <- Sys.getenv("MYSQL_HOST")
port_env <- Sys.getenv("MYSQL_PORT")
user_env <- Sys.getenv("MYSQL_USER")
#con <- dbConnect(RMySQL::MySQL(), dbname = "landing", host=host_env, port=port_env,
#                 username = user_env, password = "crmysql**##")
con <- dbConnect(RMySQL::MySQL(), dbname = "landing", host="localhost", port=3307,
                 username = "root", password = "crmysql**##")

# Create metadata table
metadata <- data.frame("algorithms" = 'c("Baseline", "GAM-1", "GAM-2", "MSTS-BASIC")',
                       "train_length" = 90,
                       "test_length" = 1,
                       "window" = '0:13',
                       "statistic" = "Amt_Pos",
                       "parallel" = 1,
                       "statistic_1" = "MAE",
                       "statistic_2" = "Amt_Pos",
                       "model_choice" = "1",
                       "weight_1" = 1,
                       "weight_2" = 0,
                       stringsAsFactors = FALSE)

dbWriteTable(con, name='metadata', value = metadata, append=TRUE, row.names=FALSE)

# Create job table
id <- UUIDgenerate(use.time = TRUE)
job <- data.frame("job_id" = id,
                  "job_ts" = Sys.time(),
                  "job_name" = "demand pipeline",
                  "job_reason" = "daily run",
                  "job_processing" = 0,
                  "job_comment" = "",
                  stringsAsFactors = FALSE)
dbWriteTable(con, name='job', value = job, append=TRUE, row.names=FALSE)

# Create product table
products <- read.csv("products.csv", stringsAsFactors = F)
dbWriteTable(con, name='product', value = products, append=TRUE, row.names=FALSE)

# Disconnect
dbDisconnect(con)
