library(DBI)
library(RPostgreSQL)
library(xlsx)
library(tidyverse)

source('C:/Users/kuijt/Documents/R/Connection/R_to_MSSQL_Conn_v(prod).R')

q1 = 'SELECT TOP(100) * FROM Ring_EU_Masterdata.dbo.Master_Products'

A = dbGetQuery(msconn_rsql,q1)

uk_data = read.xlsx('C:/Dropbox (Ring)/Ring - Amsterdam/BI/2. Data extracts/Marketing/191216_users_with_doorbell_purchase_from_bf_email.xlsx', sheetName = 'uk_users', colIndex = 2, stringsAsFactors = FALSE)
eu_data = read.xlsx('C:/Dropbox (Ring)/Ring - Amsterdam/BI/2. Data extracts/Marketing/191216_users_with_doorbell_purchase_from_bf_email.xlsx', sheetName = 'eu_users', colIndex = 2, stringsAsFactors = FALSE)
uk_data$Category = 'UK'
eu_data$Category = 'EU'
data <- union_all(uk_data, eu_data)


class(uk_data$Email)

class(uk_data)



nrow(data)
nrow(uk_data)
nrow(eu_data)

colnames(eu_data)  
colnames(uk_data)

?xlsx

help(read.xlsx)
