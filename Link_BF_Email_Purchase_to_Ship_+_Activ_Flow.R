library(DBI)
library(RPostgreSQL)
library(xlsx)
library(tidyverse)

source('C:/Users/kuijt/Documents/R/Connection/R_to_MSSQL_Conn_v(prod).R')

q1 = 'SELECT TOP(100) * FROM Ring_EU_Masterdata.dbo.Master_Products'

A = dbGetQuery(msconn_rsql,q1)
