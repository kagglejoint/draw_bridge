options(java.parameters="-Xmx20g")


require("RSQLite")
require("sqldf")

device_Query = "
    SELECT  d.drawbridge_handle handle, d.device_id d_id, d.device_type d_type, d.device_os d_os, d.country d_country,
            d.anonymous_c0 d_c0, d.anonymous_c1 d_c1, d.anonymous_c2 d_c2, d.anonymous_5 d_5, d.anonymous_6 d_6, d.anonymous_7 d_7
    FROM    devices as d
    WHERE   d.drawbridge_handle <> '-1'
    "
cookies_Query = "
    SELECT  c.drawbridge_handle handle, c.cookie_id c_id, c.computer_os_type c_os, c.browser_version c_brw, c.country c_country,
            c.anonymous_c0 c_c0, c.anonymous_c1 c_c1, c.anonymous_c2 c_c2, c.anonymous_5 c_5, c.anonymous_6 c_6, c.anonymous_7 c_7
    FROM    cookies as c
    WHERE   c.drawbridge_handle <> '-1'
    "

combine_Query = "
    SELECT  d.drawbridge_handle handle, d.device_id d_id, d.device_type d_type, d.device_os d_os, d.country d_country,
            d.anonymous_c0 d_c0, d.anonymous_c1 d_c1, d.anonymous_c2 d_c2, d.anonymous_5 d_5, d.anonymous_6 d_6, d.anonymous_7 d_7,
            c.cookie_id c_id, c.computer_os_type c_os, c.browser_version c_brw, c.country c_country,
            c.anonymous_c0 c_c0, c.anonymous_c1 c_c1, c.anonymous_c2 c_c2, c.anonymous_5 c_5, c.anonymous_6 c_6, c.anonymous_7 c_7
    FROM    devices as d, cookies as c
    ON      d.drawbridge_handle = c.drawbridge_handle
    WHERE   d.drawbridge_handle <> '-1' AND  c.drawbridge_handle <> '-1'
    "

dbpath <- "F:\\MachineLeariningData\\MyKaggle\\DrawBridge\\database.sqlite"
#dbpath <- "//Volumes//USB//MachineLeariningData//MyKaggle//DrawBridge//database.sqlite"
conn <- dbConnect(RSQLite::SQLite(), dbname=dbpath)
#cdData <- dbGetQuery(conn,combine_Query)
dData <- dbGetQuery(conn,device_Query)
cData <- dbGetQuery(conn,cookies_Query)
cdData <- sqldf("SELECT * FROM cData c, dData d ON c.handle=d.handle")
#cdData$handle <- NULL
