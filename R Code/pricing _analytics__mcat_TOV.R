# Loading the required libraries/ packages that would be required for code execution
library(RJDBC)
library(xlsx)
library(readxl)
library(dplyr)
library(stringi)
library(stringr)
library(sqldf)
library(robustbase)
library(tm)
library(zoo)


# Following command will free up the heap space, so as to prevent heap error while dealing with large data
options(java.parameters = "-Xmx8000m")
options(scipen = 999)


##MCAT wise TOV analysis###

df <- read.csv("C:/Users/Imart/Downloads/mcat_bl_aov.csv",header = T)
names(df)
df1 <- df[order(df$FK_GLCAT_MCAT_ID),]

df1 <- df1[c(1:7127265),-6]
df1 <- head(df1,n=7127265)
df1 <- df1[,c(3,1,2,4,5)]
#Count of total MCAT IDs 81479
length(unique(df1$FK_GLCAT_MCAT_ID))

# Breaking data MCAT wise

MCAT_Break <- split(df1,f=df1$FK_GLCAT_MCAT_ID)


#Creating an Empty data frame for statistical values:

Stats1 <- data.frame(MCAT_ID= numeric(),
                     Min=numeric(),
                     Max=numeric(),
                     Mean=numeric(),
                     Median=numeric(),
                     OFR_IDs_Count=numeric(),
                     Q1=numeric(),
                     Q3=numeric(),
                     IQR=numeric(),
                     MC=numeric(),
                     lower=numeric(),
                     upper=numeric(),
                     outlier_cnt=numeric(),
                     outlier_per=numeric())

i<-1
total_ofrIDs<-0



### Function to get the dataframe without desired empty column#################
completeFun <- function(data, desiredCols) {
  completeVec <- complete.cases(data[, desiredCols])
  return(data[completeVec, ])
}

#### Function to assign outlier flag as 1 to any observation ######
my_populate<-function(x,lower,upper)
{
  if(x<lower|| x>upper)
  {
    return(0)# Zero Corresponds to outliers
  }
  else
  {
    return(1)
  }
}

##### Function to get mode of data ########
getmode <- function(v) {
  uniqv <- unique(v)
  uniqv[which.max(tabulate(match(v, uniqv)))]
}

#Loop for each MCAT and calculate all required values


for (i in 1:length(MCAT_Break)) {
  #Assign MCAT
  a <- MCAT_Break[[i]]
  #assigning total BL count
  total_ofrIDs <- total_ofrIDs + length(a$MID_VAL)
  
  # Check if there any BL exist for each MCATs
  length_price<-length(MCAT_Break[[i]]$ETO_OFR_DISPLAY_ID)
  
  if(length_price)
  {
    #Store Price Value
    Price<-MCAT_Break[[i]]$MID_VAL
    # Convert prices to numeric if character any
    Price<-as.numeric(MCAT_Break[[i]]$MID_VAL)
    # Sort the price value
    Price<-sort(Price)
    # Assign 0 to price where price is NA
    Price[is.na(Price)]<-0
    #Remove zero prices
    Price<-Price[Price>0]
    # Calculate MC,Q1,Q3,IQR and lower upper cap
    MC<-mc(Price,na.rm = TRUE)
    Q1 <- quantile(Price, na.rm = TRUE)[[2]]
    Q3 <- quantile(Price, na.rm = TRUE)[[4]]
    IQR <- Q3 - Q1
    if(MC<0)
    {
      
      lower <- as.numeric(Q1 - 1.5*exp(-4*MC)*IQR)
      upper <- as.numeric(Q3 + 1.5*exp(3.5*MC)*IQR)
    }
    if(MC>=0)
    {
      lower <- as.numeric(Q1 - 1.5*exp(-3.5*MC)*IQR)
      upper <- as.numeric(Q3 + 1.5*exp(4*MC)*IQR)
    }
    # Store MCAT data in data frame named a
    a<-MCAT_Break[[i]]
    a$MID_VAL<-as.numeric(a$MID_VAL)
    # Apply completeFun as created above for column Price
    a<-completeFun(a, "MID_VAL")
    a$MID_VAL<-round(a$MID_VAL)
    # Reindexing of rows
    rownames(a) <- 1:nrow(a)
    # Apply my_populate function to assign outlier flag to observation
    a$check<-apply(a,1,function(params)my_populate(as.numeric(params[3]),lower,upper))
    # If Lower value as calculated by Adjusted Box Plot is negative, remove lower two percentile values and assign lowest value to lower value
    if(lower<0)
    {
      a$MID_VAL<-as.numeric(a$MID_VAL)
      lower_data<-quantile(a$MID_VAL,0.02)
      #lower_data<-min(a[a$MID_VAL>=quantile(a$MID_VAL,0.02),c(3)])
      lower<-lower_data
    }
    
    max=max(a$MID_VAL,na.rm = T)

    if(upper>max)
    {
      a$MID_VAL <- as.numeric(a$MID_VAL)
      upper_data <- quantile(a$MID_VAL,0.98)
      upper <- upper_data
    }
        # Check outlier count and its percentage per subcat
    count_outliers<-sum(a$check==0)
    outliers_per<-(count_outliers/nrow(a))*100
    # Appending values to empty dataframe as created above
    Stats1<-rbind(Stats1,data.frame(MCAT_ID=unique(a$FK_GLCAT_MCAT_ID),min=min(a$MID_VAL,na.rm = T),
                                    max=max(a$MID_VAL,na.rm = T),mean=mean(a$MID_VAL,na.rm = T),median=median(a$MID_VAL,na.rm = T),
                                    OFR_IDs_Count=nrow(a),Q1=Q1,Q3=Q3,IQR=IQR,MC=MC,lower=lower,upper=upper,outlier_cnt=count_outliers,outlier_per=outliers_per))
    
  }
  
}

#for (i in 1:nrow(Stats1)) {
 # ifelse(is.infinite(Stats1$lower[i])==TRUE,Stats1$lower[i] <- as.numeric(Stats1$mean[i]),Stats1$lower[i] <- as.numeric(Stats1$lower[i]))
#}
#Stats1 <- NULL
#a <- NULL


write.csv(final_1,"E:/Pricing_analytics/mcat_wise_AOV-price_0_2_updated.csv",row.names = F)

MCAT_ID_TO_NAME <- readxl::read_excel("E:/Pricing_analytics/mcat_id-to_mcat_name.xlsx",sheet = 1)
MCAT <- MCAT_ID_TO_NAME[,1:2]

final_1 <- merge(x=Stats1,y=MCAT,by.x = 'MCAT_ID',by.y = 'GLCAT_MCAT_ID',all.x = T)


#*******************************##PMCAT wise TOV analysis:##**********************************#

MCAT_to_PMCAT <- readxl::read_excel("E:/Pricing_analytics/mcat_to_pmcat.xlsx",sheet = 1)

df2 <- merge(x=df1,y=MCAT_to_PMCAT,by.x = "FK_GLCAT_MCAT_ID",by.y = "FK_CHILD_MCAT_ID",all.x = T)

#df2 <- dplyr::left_join(x=df1,y=MCAT_to_PMCAT,by=c("FK_GLCAT_MCAT_ID","FK_CHILD_MCAT_ID"))

sum(is.na(df2$FK_PARENT_MCAT_ID))
df3 <- df2[,c(7,1:5)]
df2 <- df2[,c(6,1:5)]

#write.csv(df2,"E:/Pricing_analytics/all_mcat_pmcat_tov_dump.csv",row.names = F,quote = F)

df2 <- df2[!is.na(df2$FK_PARENT_MCAT_ID),]

##Get stats data for TOV :


### Function to get the dataframe without desired empty column#################
completeFun <- function(data, desiredCols) {
  completeVec <- complete.cases(data[, desiredCols])
  return(data[completeVec, ])
}

#### Function to assign outlier flag as 1 to any observation ######
my_populate<-function(x,lower,upper)
{
  if(x<lower|| x>upper)
  {
    return(0)# Zero Corresponds to outliers
  }
  else
  {
    return(1)
  }
}

##### Function to get mode of data ########
getmode <- function(v) {
  uniqv <- unique(v)
  uniqv[which.max(tabulate(match(v, uniqv)))]
}


PMCAT_Break <- split(df2,f=df2$FK_PARENT_MCAT_ID)


#Creating an Empty data frame for statistical values:

Stats2 <- data.frame(PMCAT_ID= numeric(),
                     Min=numeric(),
                     Max=numeric(),
                     Mean=numeric(),
                     Median=numeric(),
                     OFR_IDs_Count=numeric(),
                     Q1=numeric(),
                     Q3=numeric(),
                     IQR=numeric(),
                     MC=numeric(),
                     lower=numeric(),
                     upper=numeric(),
                     outlier_cnt=numeric(),
                     outlier_per=numeric())

i<-1
total_ofrIDs<-0

#Loop for each MCAT and calculate all required values


for (i in 1:length(PMCAT_Break)) {
  #Assign MCAT
  a <- PMCAT_Break[[i]]
  #assigning total BL count
  total_ofrIDs <- total_ofrIDs + length(a$MID_VAL)
  
  # Check if there any BL exist for each MCATs
  length_price<-length(PMCAT_Break[[i]]$ETO_OFR_DISPLAY_ID)
  
  if(length_price)
  {
    #Store Price Value
    Price<-PMCAT_Break[[i]]$MID_VAL
    # Convert prices to numeric if character any
    Price<-as.numeric(PMCAT_Break[[i]]$MID_VAL)
    # Sort the price value
    Price<-sort(Price)
    # Assign 0 to price where price is NA
    Price[is.na(Price)]<-0
    #Remove zero prices
    Price<-Price[Price>0]
    # Calculate MC,Q1,Q3,IQR and lower upper cap
    MC<-mc(Price,na.rm = TRUE)
    Q1 <- quantile(Price, na.rm = TRUE)[[2]]
    Q3 <- quantile(Price, na.rm = TRUE)[[4]]
    IQR <- Q3 - Q1
    if(MC<0)
    {
      
      lower <- as.numeric(Q1 - 1.5*exp(-4*MC)*IQR)
      upper <- as.numeric(Q3 + 1.5*exp(3.5*MC)*IQR)
    }
    if(MC>=0)
    {
      lower <- as.numeric(Q1 - 1.5*exp(-3.5*MC)*IQR)
      upper <- as.numeric(Q3 + 1.5*exp(4*MC)*IQR)
    }
    # Store MCAT data in data frame named a
    a<-PMCAT_Break[[i]]
    a$MID_VAL<-as.numeric(a$MID_VAL)
    # Apply completeFun as created above for column Price
    a<-completeFun(a, "MID_VAL")
    a$MID_VAL<-round(a$MID_VAL)
    # Reindexing of rows
    rownames(a) <- 1:nrow(a)
    # Apply my_populate function to assign outlier flag to observation
    a$check<-apply(a,1,function(params)my_populate(as.numeric(params[4]),lower,upper))
    # If Lower value as calculated by Adjusted Box Plot is negative, remove lower two percentile values and assign lowest value to lower value
    if(lower<0)
    {
      a$MID_VAL<-as.numeric(a$MID_VAL)
      lower_data<-min(a[a$MID_VAL>quantile(a$MID_VAL,0.02),c(4)])
      lower<-lower_data
    }
    # Check outlier count and its percentage per subcat
    count_outliers<-sum(a$check==0)
    outliers_per<-(count_outliers/nrow(a))*100
    # Appending values to empty dataframe as created above
    Stats2<-rbind(Stats2,data.frame(PMCAT_ID=unique(a$FK_PARENT_MCAT_ID),min=min(a$MID_VAL,na.rm = T),
                                    max=max(a$MID_VAL,na.rm = T),mean=mean(a$MID_VAL,na.rm = T),median=median(a$MID_VAL,na.rm = T),
                                    OFR_IDs_Count=nrow(a),Q1=Q1,Q3=Q3,IQR=IQR,MC=MC,lower=lower,upper=upper,outlier_cnt=count_outliers,outlier_per=outliers_per))
    
  }
  
}


write.csv(Stats2,"E:/Pricing_analytics/PMCAT_wise_AOV-price.csv",row.names = F)


#*******************************##SUBCAT wise TOV analysis:##**********************************#

df3 <- df3[!is.na(df3$SUBCAT_ID),]

df3 <- df3[df3$SUBCAT_ID!=0,]
SUBCAT_BREAK <- split(df3,f=df3$SUBCAT_ID)


#Creating an Empty data frame for statistical values:

Stats3 <- data.frame(SUBCAT_ID= numeric(),
                     Min=numeric(),
                     Max=numeric(),
                     Mean=numeric(),
                     Median=numeric(),
                     OFR_IDs_Count=numeric(),
                     Q1=numeric(),
                     Q3=numeric(),
                     IQR=numeric(),
                     MC=numeric(),
                     lower=numeric(),
                     upper=numeric(),
                     outlier_cnt=numeric(),
                     outlier_per=numeric())

i<-1
total_ofrIDs<-0

#Loop for each MCAT and calculate all required values


for (i in 1:length(SUBCAT_BREAK)) {
  #Assign MCAT
  a <- SUBCAT_BREAK[[i]]
  #assigning total BL count
  total_ofrIDs <- total_ofrIDs + length(a$MID_VAL)
  
  # Check if there any BL exist for each MCATs
  length_price<-length(SUBCAT_BREAK[[i]]$ETO_OFR_DISPLAY_ID)
  
  if(length_price)
  {
    #Store Price Value
    Price<-SUBCAT_BREAK[[i]]$MID_VAL
    # Convert prices to numeric if character any
    Price<-as.numeric(SUBCAT_BREAK[[i]]$MID_VAL)
    # Sort the price value
    Price<-sort(Price)
    # Assign 0 to price where price is NA
    Price[is.na(Price)]<-0
    #Remove zero prices
    Price<-Price[Price>0]
    # Calculate MC,Q1,Q3,IQR and lower upper cap
    MC<-mc(Price,na.rm = TRUE)
    Q1 <- quantile(Price, na.rm = TRUE)[[2]]
    Q3 <- quantile(Price, na.rm = TRUE)[[4]]
    IQR <- Q3 - Q1
    if(MC<0)
    {
      
      lower <- as.numeric(Q1 - 1.5*exp(-4*MC)*IQR)
      upper <- as.numeric(Q3 + 1.5*exp(3.5*MC)*IQR)
    }
    if(MC>=0)
    {
      lower <- as.numeric(Q1 - 1.5*exp(-3.5*MC)*IQR)
      upper <- as.numeric(Q3 + 1.5*exp(4*MC)*IQR)
    }
    # Store MCAT data in data frame named a
    a<-SUBCAT_BREAK[[i]]
    a$MID_VAL<-as.numeric(a$MID_VAL)
    # Apply completeFun as created above for column Price
    a<-completeFun(a, "MID_VAL")
    a$MID_VAL<-round(a$MID_VAL)
    # Reindexing of rows
    rownames(a) <- 1:nrow(a)
    # Apply my_populate function to assign outlier flag to observation
    a$check<-apply(a,1,function(params)my_populate(as.numeric(params[4]),lower,upper))
    # If Lower value as calculated by Adjusted Box Plot is negative, remove lower two percentile values and assign lowest value to lower value
    if(lower<0)
    {
      a$MID_VAL<-as.numeric(a$MID_VAL)
      lower_data<-min(a[a$MID_VAL>quantile(a$MID_VAL,0.02),c(4)])
      lower<-lower_data
    }
    # Check outlier count and its percentage per subcat
    count_outliers<-sum(a$check==0)
    outliers_per<-(count_outliers/nrow(a))*100
    # Appending values to empty dataframe as created above
    Stats3<-rbind(Stats3,data.frame(SUBCAT_ID=unique(a$SUBCAT_ID),min=min(a$MID_VAL,na.rm = T),
                                    max=max(a$MID_VAL,na.rm = T),mean=mean(a$MID_VAL,na.rm = T),median=median(a$MID_VAL,na.rm = T),
                                    OFR_IDs_Count=nrow(a),Q1=Q1,Q3=Q3,IQR=IQR,MC=MC,lower=lower,upper=upper,outlier_cnt=count_outliers,outlier_per=outliers_per))
    
  }
  
}


write.csv(Stats3,"E:/Pricing_analytics/SUBCAT_wise_AOV-price.csv",row.names = F)



#************************ Creating file format TBL Sold and UBL sold data   *************************#

#reading files

complete_data <- read.csv("C:/Users/Imart/Downloads/bltotal (1).csv")

length(unique(complete_data$FK_GLCAT_MCAT_ID))
length(unique(complete_data$FK_GL_CITY_ID))
sum(complete_data$UNQ_BL_SOLD,na.rm = T)
sum(complete_data$TOTAL_TRANSACTION,na.rm = T)
sum(complete_data$TOTAL_APPROVED_BUY_LEAD,na.rm = T)
length((complete_data$FK_GLCAT_MCAT_ID))
sum(is.na(complete_data$FK_GLCAT_MCAT_ID))

colnames(complete_data)
res1 <- complete_data[,c('FK_GLCAT_MCAT_ID','FK_GL_CITY_ID','TOTAL_APPROVED_BUY_LEAD','UNQ_BL_SOLD','TOTAL_TRANSACTION')]
res1 <- res1[!is.na(res1$FK_GLCAT_MCAT_ID),]

MCAT_DATA <- readxl::read_excel("E:/Pricing_analytics/mcat_idvsMcat_name.xlsx",sheet = 1)
#res1 <- dplyr::left_join(res1,MCAT_DATA,by=c("FK_GLCAT_MCAT_ID","GLCAT_MCAT_ID"))
res1 <- merge(x=res1,y=MCAT_DATA,by.x = 'FK_GLCAT_MCAT_ID',by.y = 'GLCAT_MCAT_ID',all.x = T)
res1 <- res1[,c('FK_GLCAT_MCAT_ID','GLCAT_MCAT_NAME','FK_GL_CITY_ID','TOTAL_APPROVED_BUY_LEAD','UNQ_BL_SOLD','TOTAL_TRANSACTION')]

#loading city_data to get_city name

CITY_DATA <- readxl::read_excel("E:/Pricing_analytics/CITY_DETAILS.xlsx",sheet = 1)
CITY_DATA <- CITY_DATA[,c(1,2)]
colnames(CITY_DATA)

res1 <- merge(x=res1,y=CITY_DATA,by.x = 'FK_GL_CITY_ID',by.y = 'GL_CITY_ID',all.x = T)
res1 <- res1[,c('FK_GLCAT_MCAT_ID','GLCAT_MCAT_NAME','FK_GL_CITY_ID','GL_CITY_NAME','TOTAL_APPROVED_BUY_LEAD','UNQ_BL_SOLD','TOTAL_TRANSACTION')]

#NOW_READING Q3 FROM PREVIOUS ANALYSIS

Q3_DATA <- readxl::read_excel("E:/Pricing_analytics/mcat_wise_AOV-price_final.xlsx",sheet = 1)
Q3_DATA <- Q3_DATA[,c('MCAT_ID',"Q3")]
res1 <- merge(x=res1,y=Q3_DATA,by.x = "FK_GLCAT_MCAT_ID",by.y ="MCAT_ID",all.x = T )


#Replacing NAs with 0 in TOtal_BL, BL_sold and Unique_BL_sold data:

res1[is.na(res1$UNQ_BL_SOLD),c('UNQ_BL_SOLD')] <- 0
res1[is.na(res1$TOTAL_TRANSACTION),c('TOTAL_TRANSACTION')] <- 0

#creating two seperate columns for TBL/BLA and UBL/BLA

res1$Sold_Ratio_TBL <- res1$TOTAL_TRANSACTION/res1$TOTAL_APPROVED_BUY_LEAD
res1$Sold_Ratio_UBL <- res1$UNQ_BL_SOLD/res1$TOTAL_APPROVED_BUY_LEAD
res1$Sold_Ratio_TBL<-formatC(res1$Sold_Ratio_TBL,digits = 2,format = 'f')
res1$Sold_Ratio_UBL<-formatC(res1$Sold_Ratio_UBL,digits = 2,format = 'f')


write.csv(res1,"E:/Pricing_analytics/pricing_city_wiseQ3.csv",row.names = F,quote = F)


#****Joins with MCAT wise City wise Paid seller count****#

City_AOV <- res1


Final_combined_data <- merge(x=City_AOV,y=MCAT_city_combine,by.x = c("FK_GLCAT_MCAT_ID","GL_CITY_NAME"),by.y = c("FK_GLCAT_MCAT_ID","GLUSR_USR_CITY"),all.x = T)
sum(grepl(",",Final_combined_data$GL_CITY_NAME)==T)
Index_1 <- which(grepl(",",Final_combined_data$GLCAT_MCAT_NAME)==T)
Final_combined_data$GL_CITY_NAME <- gsub(","," ",Final_combined_data$GL_CITY_NAME)
Final_combined_data$GLCAT_MCAT_NAME <- gsub(","," ",Final_combined_data$GLCAT_MCAT_NAME)
MCAT_with_COMMA <- Final_combined_data[Index_1,]
MCAT_with_COMMA <- MCAT_with_COMMA[!duplicated(MCAT_with_COMMA$FK_GLCAT_MCAT_ID),]
Final_combined_data$FK_GLCAT_MCAT_ID[472928]
colnames(Final_combined_data)[names(Final_combined_data)=="COUNT(GLUSR_USR_ID)"] <- "Paid_Seller_Count"
Final_combined_data[is.na(Final_combined_data$Paid_Seller_Count),c("Paid_Seller_Count")] <- 0

write.csv(check2,"E:/Pricing_analytics/final_AOV_data_city_paid_seller.csv",row.names = F,quote = F)
memory.limit(size = 800000)
#Final2 <- merge(x=City_AOV,y=MCAT_city_combine,by.x = "FK_GLCAT_MCAT_ID",by.y = "FK_GLCAT_MCAT_ID",all.x = T)


check2 <- read.csv("E:/Pricing_analytics/final_AOV_data_city_paid_seller.csv")

SUBCAT_DATA <- readxl::read_excel("E:/Pricing_analytics/mcat_id_to_subcat_id.xlsx",sheet = 1)

City_state <- readxl::read_excel("E:/Pricing_analytics/city_state and tier.xlsx",sheet = 1)

Processed1 <- merge(x=check2,y=SUBCAT_DATA,by.x = 'FK_GLCAT_MCAT_ID',by.y = 'FK_GLCAT_MCAT_ID',all.x = T,all.y = F)

Final_data <- merge(x=Processed1,y=City_state,by.x = c('FK_GL_CITY_ID','GL_CITY_NAME'),by.y = c('GL_CITY_ID','GL_CITY_NAME'),all.x = T)

Final_data <- Final_data[,c(12,3,4,1,2,13:15,5:11)]
colnames(Final_data)[names(Final_data)=="Q3"] <- "Q3-AOV"
sum(grepl(",",Final_data$GL_STATE_NAME)==T)
sum(grepl(",",Final_data$GLCAT_MCAT_NAME)==T)
write.csv(Final_data,"E:/Pricing_analytics/MCAT_AOV_City_seller_final.csv",row.names = F,quote = F)



check <- read.csv("E:/Pricing_analytics/MCAT_AOV_City_seller_final.csv")
check2 <- check[check$GLCAT_MCAT_NAME=='VFD',]
check <- check[,-15]


library(RODBC)
library(RJDBC)
library(xlsx)
library(dplyr)
library(sqldf)
# Creating greater heap space

options(java.parameters = "-Xmx8000m")

# Creating variables startDate and endDate
startDate<-Sys.Date()-7# Format here is 2018-04-17
endDate<- Sys.Date()-1
# Converting into suitable format
startDate <-format(startDate,format="%d-%b-%y")# Format here is 17-Apr-18
endDate <-format(endDate,format="%d-%b-%y")

## Create JDBC Connection
jdbcDriver <- JDBC(driverClass="oracle.jdbc.OracleDriver", classPath="C:/Users/Imart/Desktop/sqldeveloper/jdbc/lib/ojdbc6.jar")

## Create IMBLR Connection to DB
jdbcConIMBLR <- dbConnect(jdbcDriver, "jdbc:oracle:thin:@//206.191.151.214:1521/IMBL", "indiamart", "blalrtdb4iil")



getID <- paste0("select FK_GLCAT_MCAT_ID,GLUSR_USR_CITY,count(GLUSR_USR_ID)
from eto_trd_alert_v2,glusr_usr
                where eto_trd_alert_v2.FK_GLUSR_USR_ID = glusr_usr.GLUSR_USR_ID
                --and GLUSR_USR_PAID_SERV =1
                and GLUSR_USR_CUSTTYPE_NAME in ('TSCATALOG',
                'LEADER',
                'STAR',
                'CATALOG',
                'BL Paid VFCP',
                'OthersPAID VFCP',
                'BL Paid FCP',
                'Catalog PNS Defaulter',
                'OthersPAID NoFCP')
                --and FK_GLCAT_MCAT_ID = 2699
                group by FK_GLCAT_MCAT_ID,GLUSR_USR_CITY")

QueryResult <- dbGetQuery(jdbcConIMBLR,getID)
write.csv(QueryResult,"E:/Pricing_analytics/mcat_city_wise_paid_seller.csv",row.names = F,quote = F)
Final_Result <- merge(x=check,y=QueryResult,by.x = c('FK_GLCAT_MCAT_ID','GL_CITY_NAME'),by.y = c('FK_GLCAT_MCAT_ID','GLUSR_USR_CITY'),all.x = T)


Final_Result[is.na(Final_Result$`COUNT(GLUSR_USR_ID)`),15] <- 0

ckck <- Final_Result[Final_Result$GLCAT_MCAT_NAME=='VFD',]
ckck <- ckck[,c(1:6,15,7:14)]
colnames(ckck)[colnames(ckck)=="COUNT(GLUSR_USR_ID)"] <- "Paid_Seller_Count"

colnames(Final_Result)
colnames(Final_Result)[colnames(Final_Result)=="COUNT(GLUSR_USR_ID)"] <- "Paid_Seller_Count"

write.csv(Final_Result,"E:/Pricing_analytics/MCAT_AOV_City_seller_final_new.csv",row.names = F,quote = F)
Final_Result <- Final_Result[,c(3,1,4,5,2,6:15)]
