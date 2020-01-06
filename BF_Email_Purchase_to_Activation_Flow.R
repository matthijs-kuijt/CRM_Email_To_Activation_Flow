library(DBI)
library(RPostgreSQL)
library(xlsx)
library(tidyverse)


#Connect to MSSQL
source('C:/Users/kuijt/Documents/R/Connection/R_to_MSSQL_Conn_v(prod).R')


#Connect to Redshift
source('C:/Users/kuijt/Documents/R/Connection/R_to_RS_Conn_v(prod).R')


runtime_start = Sys.time()

#SET INPUT
#Set filter on products
product_family = 'Doorbells'
#Input Excel file (Ertan)
input_excel = 'C:/Dropbox (Ring)/Ring - Amsterdam/BI/2. Data extracts/Marketing/191219_users_with_doorbell_purchase_from_bf_email.xlsx'


#Load Email Purchases Data from Excel files
uk_data = read.xlsx(input_excel, sheetName = 'uk_users', colIndex = c(2:6), stringsAsFactors = FALSE)
eu_data = read.xlsx(input_excel, sheetName = 'eu_users', colIndex = c(2:6), stringsAsFactors = FALSE)
uk_data$category = 'UK'
eu_data$category = 'EU'
email_data <- union_all(uk_data, eu_data) %>%
  `colnames<-`(c('order_number', 'email_address', 'order_date', 'quantity_sold', 'SKU', 'category')) %>%
  mutate(order_date = as.Date(order_date))


#Load Shipment Data
start_order_date = min(email_data$order_date)

q1 = paste("SELECT		so.order_date, so.order_number, so.email_address, so.fulfillment_status, so.financial_status, ieh.Actual_Post_Goods_Issue_Date, ieh.Serial_Number, ieh.Material AS SKU, mp.Product_Family, mp.Product_Category, mp.Product_Name_Full
FROM		Ring_EU_Operations.dbo.RAW_IMUK_ExecutedOrders_History ieh
LEFT JOIN	Ring_EU_Masterdata.dbo.Master_Products mp ON ieh.Material = mp.SKU
LEFT JOIN	Ring_EU_Ecommerce.dbo.Shopify_Orders so ON ieh.Customer_ref = so.order_number
WHERE		1=1
AND			so.order_number IS NOT NULL
AND     mp.product_family = '", product_family, "'
AND			so.order_date >= '", start_order_date, "'

UNION ALL

SELECT		so.order_date, so.order_number, so.email_address, so.fulfillment_status, so.financial_status, ieh.Actual_Post_Goods_Issue_Date, ieh.Serial_Number, ieh.Material AS SKU, mp.Product_Family, mp.Product_Category, mp.Product_Name_Full
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
  mutate(Serial_Number = toupper(Serial_Number)) %>%
  select(order_date.x, category, order_number, email_address, fulfillment_status, financial_status, quantity_sold, SKU.x, Actual_Post_Goods_Issue_Date, Serial_Number, Product_Family, Product_Category, Product_Name_Full) %>%
  `colnames<-` (c('order_date', 'category', 'order_number', 'order_email', 'fulfillment_status', 'financial_status', 'quantity_sold', 'sku', 'shipment_date', 'mac_id', 'product_family', 'product_category', 'product_name_full'))


#Query activation data
q2 = paste("SELECT      dd.id, dd.doorbot_web_doorbot_id, dd.activation_time::TIMESTAMP AS activation_time, dhi.device_type, dhi.device_category, dhi.marketing_name, COALESCE(UPPER(md.mac_id), UPPER(dd.mac_address)) AS mac_id, UPPER(md.serial_number) AS serial_number, hdo.household_id, dwu.email AS activation_email, hdo.device_order, hdo.ownership_time::TIMESTAMP AS ownership_time, dds.location_id, dc.iso_code, dc.country AS activation_country
FROM        bi_edw_prod.dim_device dd
           LEFT JOIN   bi_edw_prod.device_hierarchy dhi ON dd.device_type = dhi.device_type
           LEFT JOIN   (SELECT DISTINCT mac_id, serial_number FROM bi_raw_prod.manufactured_devices) md ON UPPER(dd.mac_address) = UPPER(md.mac_id)
           LEFT JOIN   bi_edw_prod.dim_household_device_order hdo ON dd.id = hdo.device_id
           LEFT JOIN   (
           SELECT MAX(operating_day) AS max_operating_day, household_id, device_id
           FROM bi_edw_prod.daily_device_summary dds
           WHERE location_id IS NOT NULL
           GROUP BY 2,3
           ) dds_max ON hdo.household_id = dds_max.household_id AND hdo.device_id = dds_max.device_id
           INNER JOIN  bi_edw_prod.daily_device_summary dds ON dds_max.max_operating_day = dds.operating_day AND dds_max.household_id = dds.household_id AND dds_max.device_id = dds.device_id
           LEFT JOIN   bi_raw_prod.locations l ON dds.location_id = l.location_id
           LEFT JOIN   bi_edw_prod.dim_country dc ON l.country = dc.iso_code
           LEFT JOIN   bi_edw_prod.dim_household_scrubbed dh ON hdo.household_id = dh.id
           LEFT JOIN   bi_raw_prod.doorbot_web_users dwu ON dh.doorbot_web_user_id = dwu.id
           WHERE       1=1
           AND         hdo.ownership_time >= '", start_order_date, "'
           AND         md.mac_id IS NOT NULL
           --Filter out US to keep activation table relatively small, will exclude devices bought in Europe but activated in US from analysis
           AND dc.country <> 'US' AND dc.country <> 'United States'           
           ORDER BY    hdo.household_id, hdo.ownership_time
           ", sep = "")


activation_data = dbGetQuery(pconn_rsql,q2)

#Activation table: union results when joining on mac_id vs serial_number (as shipping mac_id can represent both due to DQ)
activations <- union_all(
                          (
                            #join on mac_id
                            left_join(email_ship_data,activation_data, by = "mac_id") %>%
                            select(c((ncol(email_ship_data)+1):(ncol(email_ship_data)+ncol(activation_data)-1), 'mac_id')) %>%
                            filter(device_order >= 1)
                          )
                          ,
                          (
                            #join on serial_number
                            left_join(email_ship_data,(activation_data %>% add_column(serial_number_join = activation_data$serial_number)), by = c("mac_id" =  "serial_number_join")) %>%
                            select(c((ncol(email_ship_data)+1):(ncol(email_ship_data)+ncol(activation_data)), 'mac_id')) %>%
                            filter(device_order >= 1) %>%
                            select(-c('mac_id.y'))
                          )
                        ) 


#Join email_ship_data with activations
final_df <- left_join(email_ship_data, activations, by = 'mac_id') %>%
  arrange(!is.na(fulfillment_status),fulfillment_status, shipment_date, ownership_time)


#Saving output (backend version)
save_loc_backend = paste('C:/Users/kuijt/Documents/R/Email to Activation Flow/',paste(substr(Sys.Date(),3,4),substr(Sys.Date(),6,7),substr(Sys.Date(),9,10),sep=""),'_BF_Email_Activation_Data_v(backend).xlsx', sep="")
write.xlsx(final_df, save_loc_backend, row.names = FALSE)


#Automatically adjusting column widths (backend version)
wb <- loadWorkbook(save_loc_backend)
sheets <- getSheets(wb)
autoSizeColumn(sheets[[1]], colIndex=1:ncol(final_df))
saveWorkbook(wb,save_loc_backend)


#Saving output (end user version)
save_loc_output = paste('C:/Users/kuijt/Documents/R/Email to Activation Flow/',paste(substr(Sys.Date(),3,4),substr(Sys.Date(),6,7),substr(Sys.Date(),9,10),sep=""),'_BF_Email_Activation_Data_v(output).xlsx', sep="")
write.xlsx(final_df %>% select(order_number, mac_id, sku, product_family, product_category, product_name_full, order_date, quantity_sold, order_email, shipment_date, activation_email, activation_time, ownership_time, device_order, activation_country), save_loc_output, row.names = FALSE)


#Automatically adjusting column widths (end user version)
wb <- loadWorkbook(save_loc_output)
sheets <- getSheets(wb)
autoSizeColumn(sheets[[1]], colIndex=1:ncol(final_df))
saveWorkbook(wb,save_loc_output)


#Print Runtime
runtime_end = Sys.time()
print(runtime_end - runtime_start) 