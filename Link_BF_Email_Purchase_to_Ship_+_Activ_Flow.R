library(DBI)
library(RPostgreSQL)
library(xlsx)
library(tidyverse)


#Set filter on products
product_family = 'Doorbells'


#Load Email Purchases Data from Excel files
uk_data = read.xlsx('C:/Dropbox (Ring)/Ring - Amsterdam/BI/2. Data extracts/Marketing/191219_users_with_doorbell_purchase_from_bf_email.xlsx', sheetName = 'uk_users', colIndex = c(2:6), stringsAsFactors = FALSE)
eu_data = read.xlsx('C:/Dropbox (Ring)/Ring - Amsterdam/BI/2. Data extracts/Marketing/191219_users_with_doorbell_purchase_from_bf_email.xlsx', sheetName = 'eu_users', colIndex = c(2:6), stringsAsFactors = FALSE)
uk_data$category = 'UK'
eu_data$category = 'EU'
email_data <- union_all(uk_data, eu_data) %>%
  `colnames<-`(c('order_number', 'email_address', 'order_date', 'quantity_sold', 'SKU', 'category')) %>%
  mutate(order_date = as.Date(order_date))


#Connect to MSSQL
source('C:/Users/kuijt/Documents/R/Connection/R_to_MSSQL_Conn_v(prod).R')


#Load Shipment Data
start_order_date = min(email_data$order_date)

q1 = paste("SELECT		so.order_date, so.order_number, so.email_address, ieh.Actual_Post_Goods_Issue_Date, ieh.Serial_Number, ieh.Material AS SKU, mp.Product_Family, mp.Product_Category, mp.Product_Name_Full
FROM		Ring_EU_Operations.dbo.RAW_IMUK_ExecutedOrders_History ieh
LEFT JOIN	Ring_EU_Masterdata.dbo.Master_Products mp ON ieh.Material = mp.SKU
LEFT JOIN	Ring_EU_Ecommerce.dbo.Shopify_Orders so ON ieh.Customer_ref = so.order_number
WHERE		1=1
AND			so.order_number IS NOT NULL
AND     mp.product_family = '", product_family, "'
AND			so.order_date >= '", start_order_date, "'

UNION ALL

SELECT		so.order_date, so.order_number, so.email_address, ieh.Actual_Post_Goods_Issue_Date, ieh.Serial_Number, ieh.Material AS SKU, mp.Product_Family, mp.Product_Category, mp.Product_Name_Full
FROM		Ring_EU_Operations.dbo.RAW_IMNL_ExecutedOrders_History ieh
LEFT JOIN	Ring_EU_Masterdata.dbo.Master_Products mp ON ieh.Material = mp.SKU
LEFT JOIN	Ring_EU_Ecommerce.dbo.Shopify_Orders so ON ieh.Customer_ref = so.order_number
WHERE		1=1
AND			so.order_number IS NOT NULL
AND     mp.product_family = '", product_family, "'
AND			so.order_date >= '", start_order_date, "'", sep = "")

ship_data <- dbGetQuery(msconn_rsql,q1) %>%
  mutate(order_date = as.Date(order_date),
         Actual_Post_Goods_Issue_Date = as.Date(Actual_Post_Goods_Issue_Date))


#Connecting Email data to Shipment data
email_ship_data <- left_join(email_data, ship_data, by = c('order_number', 'email_address')) %>%
  select(order_date.x, category, order_number, email_address, quantity_sold, SKU.x, Actual_Post_Goods_Issue_Date, Serial_Number, Product_Family, Product_Category, Product_Name_Full) %>%
  `colnames<-` (c('order_date', 'category', 'order_number', 'email_address', 'quantity_sold', 'sku', 'shipment_date', 'mac_id', 'product_family', 'product_category', 'product_name_full'))


#Saving output
save_loc = paste('C:/Users/kuijt/Documents/R/Email to Activation Flow/',paste(substr(Sys.Date(),3,4),substr(Sys.Date(),6,7),substr(Sys.Date(),9,10),sep=""),'_BF_Email_Shipment_Data.xlsx', sep="")
write.xlsx(email_ship_data, save_loc, row.names = FALSE)

#Automatically adjusting column widths
wb <- loadWorkbook(save_loc)
sheets <- getSheets(wb)
autoSizeColumn(sheets[[1]], colIndex=1:ncol(email_ship_data))
saveWorkbook(wb,save_loc)

print("Done")
