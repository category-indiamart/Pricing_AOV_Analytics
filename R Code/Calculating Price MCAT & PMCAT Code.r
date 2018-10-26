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
# Following command will free up the heap space, so as to prevent heap error while dealing with large data
options(java.parameters = "-Xmx8000m")

# Loading JDBC Driver to connect the DB, here the classpath is set to ojdbc.jar 
jdbcDriver <- JDBC(driverClass="oracle.jdbc.OracleDriver", classPath="C:/Users/imart/Desktop/sqldeveloper/jdbc/lib/ojdbc6.jar")

# Connecting to MESH Database
jdbcConMesh <- dbConnect(jdbcDriver, "jdbc:oracle:thin:@//ora4-dl.intermesh.net:1521/mesh", "indiamart", "ora926meSh)%")

# Read MCAT_SUBCAT group file
id<-read_excel("C:/Users/imart/Downloads/MCAT_SUBCAT_Group_03 June.xlsx")

# Select the unique Category id from the data of MCAT Subcat imported above
id<-unique(id$CAT_ID)

#Preparing is string to pass in query, for eg id1,id2,id3...etc
id<-paste(id,collapse=",")

# Quering the Database 
if(exists("jdbcConMesh"))
{
  #Fetching Data from MEshR
  
  query1 <- paste0("Select FK_GLCAT_CAT_ID,A.FK_GLCAT_MCAT_ID, PC_ITEM_ID ,PC_ITEM_DISPLAY_ID ,PC_ITEM_NAME ,FK_PC_CLNT_ID ,PC_ITEM_FOB_PRICE ,PC_ITEM_MOQ_UNIT_TYPE, PC_ITEM_MIN_ORDER_QUANTITY
            FROM PC_ITEM_TO_GLCAT_MCAT, PC_ITEM,glcat_cat_to_mcat A
            Where FK_PC_ITEM_ID = PC_ITEM_ID  
            And PC_ITEM_FOB_PRICE is not NULL
            AND PC_ITEM_FOB_PRICE_CURRENCY = 'INR'
           AND PC_ITEM_TO_GLCAT_MCAT.FK_GLCAT_MCAT_ID = A.FK_GLCAT_MCAT_ID
          and FK_GLCAT_CAT_ID in (",id,")")
  
  meshrquery1 <- dbGetQuery(jdbcConMesh,query1);
  # Total time to run above query is 10 mins
  
}

#Making copy of data we got from DB
meshquery<-meshrquery1

# Saving Image for later use
save.image("C:/Ankit Verma/Old Laptop/Ankit Backup/Pear work/Shivendra/Pricing Analytics/SubCatData.RData")

#Loading the data as processed above
load("F:/Indiamart/Pricing Analytics/SubCatData.RData")

# Removing unnecessary objects
rm(jdbcConMesh,jdbcDriver,id,query1)

# Converting Units in DB to lowercase and trimming
meshrquery1$PC_ITEM_MOQ_UNIT_TYPE<-tolower(meshrquery1$PC_ITEM_MOQ_UNIT_TYPE)
meshrquery1$PC_ITEM_MOQ_UNIT_TYPE<-trimws(meshrquery1$PC_ITEM_MOQ_UNIT_TYPE,which = c("both", "left", "right"))

#length(unique(meshrquery1$PC_ITEM_MOQ_UNIT_TYPE))
# Getting the distinct unit types along with their frequencies
query<-"select distinct PC_ITEM_MOQ_UNIT_TYPE,COUNT(PC_ITEM_MOQ_UNIT_TYPE) FROM meshrquery1 group by PC_ITEM_MOQ_UNIT_TYPE"
data<-sqldf(query)
# Exporting data so that top 95% units can be extracted
write.csv(data,"F:/Indiamart/Pricing Analytics/units_final.csv")

# Manually Assign the Corresponding units in generated csv and save it as excel


# Importing the unit file so that it can be merged
final_units<-read_excel("F:/Indiamart/Pricing Analytics/FINAL UNIT.xlsx",sheet=1)

# Removing last two unnecessary rows
final_units<-head(final_units,n=32635)
names(final_units)
# Renaming Columns of Dataframe of units
final_units<-rename(final_units,tot_cnt=`COUNT(PC_ITEM_MOQ_UNIT_TYPE)`)
final_units<-rename(final_units,contr=`% Of Product`)
final_units<-rename(final_units,Corr_unit=`Final Unit`)
length(unique(final_units$PC_ITEM_MOQ_UNIT_TYPE))

# Keeping final_units without duplication, here we get unique units types as 32611
final_units<-final_units[!duplicated(final_units$PC_ITEM_MOQ_UNIT_TYPE), ]

# Converting Unit types to lowercase and trimming so that correctly merged
final_units$PC_ITEM_MOQ_UNIT_TYPE<-tolower(final_units$PC_ITEM_MOQ_UNIT_TYPE)
final_units$PC_ITEM_MOQ_UNIT_TYPE<-trimws(final_units$PC_ITEM_MOQ_UNIT_TYPE,which = c("both", "left", "right"))

#Merging the data units data with Existing Product Data
data<- sqldf("Select t1.*,t2.Corr_Unit from meshrquery1 t1 left join final_units t2 on t1.PC_ITEM_MOQ_UNIT_TYPE=t2.PC_ITEM_MOQ_UNIT_TYPE")

# Remove Data containing NA
#without_na_prod<-data[!is.na(data$PC_ITEM_MOQ_UNIT_TYPE),]
#na_prod<-without_na_prod[is.na(without_na_prod$PC_ITEM_MOQ_UNIT_TYPE),]
#na_prod<-without_na_prod[is.na(without_na_prod$Corr_unit),]

#without_na_prod<-without_na_prod[!is.na(without_na_prod$Corr_unit),]
#data<-without_na_prod
## Combining the data with PMCAT
#pmcat_list<-read_excel("F:/Indiamart/Pricing Analytics/MCAT_PMCAT_17 June (2).xlsx")
#data1<-sqldf("Select t1.*,t2.parentid from data t1 join pmcat_list t2 on t1.FK_GLCAT_MCAT_ID=T2.CHILDID")

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

########################### Exclusive of Unit Calculation #################################
############## Case 1 Breaking Data Subcat wise without unit consideration###################
data1<-data
# Breaking Data Subcat wise
subcat_break <- split( data1 , f = data1$FK_GLCAT_CAT_ID)
# Creating an emoty Dataframe 
stats1<-data.frame(SubCatID=numeric(),
                   min=numeric(),
                   max=numeric(),
                   mean=numeric(),
                   median=numeric(),
                   Prod_Cnt=numeric(),
                   Q1=numeric(),
                   Q3=numeric(),
                   IQR=numeric(),
                   MC=numeric(),
                   lower=numeric(),
                   upper=numeric(),
                   outlier_cnt=numeric(),
                   outlier_per=numeric())
i<-1
total_products<-0
 # Loop for each subcat, do calculation
for(i in 1:length(subcat_break))
{
# Assign subcat
 a<-subcat_break[[i]]
 # Assign total product count 
   total_products<-total_products+length(a$PC_ITEM_FOB_PRICE)
 # Check if there products exists for each Subcat
   length_price<-length(subcat_break[[i]]$PC_ITEM_NAME)
   if(length_price)
   {
      #Store Price Value
      Price<-subcat_break[[i]]$PC_ITEM_FOB_PRICE
	  # Convert prices to numeric if character any
      Price<-as.numeric(subcat_break[[i]]$PC_ITEM_FOB_PRICE)
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
	  # Store Subcat data in data frame named a
      a<-subcat_break[[i]]
      a$PC_ITEM_FOB_PRICE<-as.numeric(a$PC_ITEM_FOB_PRICE)
	  # Apply completeFun as created above for column Price
      a<-completeFun(a, "PC_ITEM_FOB_PRICE")
      a$PC_ITEM_FOB_PRICE<-round(a$PC_ITEM_FOB_PRICE)
	  # Reindexing of rows
      rownames(a) <- 1:nrow(a)
	  # Apply my_populate function to assign outlier flag to observation
      a$check<-apply(a,1,function(params)my_populate(as.numeric(params[7]),lower,upper))
	  # If Lower value as calculated by Adjusted Box Plot is negative, remove lower two percentile values and assign lowest value to lower value
      if(lower<0)
      {
         a$PC_ITEM_FOB_PRICE<-as.numeric(a$PC_ITEM_FOB_PRICE)
         lower_data<-min(a[a$PC_ITEM_FOB_PRICE>quantile(a$PC_ITEM_FOB_PRICE,0.02),c(7)])
         lower<-lower_data
      }
	  # Check outlier count and its percentage per subcat
      count_outliers<-sum(a$check==0)
      outliers_per<-(count_outliers/nrow(a))*100
	  # Appending values to empty dataframe as created above
      stats1<-rbind(stats1,data.frame(SubCatID=unique(a$FK_GLCAT_CAT_ID),min=min(a$PC_ITEM_FOB_PRICE,na.rm = T),
                                      max=max(a$PC_ITEM_FOB_PRICE,na.rm = T),mean=mean(a$PC_ITEM_FOB_PRICE,na.rm = T),median=median(a$PC_ITEM_FOB_PRICE,na.rm = T),
                                      Prod_Cnt=nrow(a),Q1=Q1,Q3=Q3,IQR=IQR,MC=MC,lower=lower,upper=upper,outlier_cnt=count_outliers,outlier_per=outliers_per))
      
   }
   
}

length(sum(stats1$Prod_Cnt))    

write.xlsx(stats1,"F:/Indiamart/Pricing Analytics/Pricing/Funnel/New Funnel/SubcatWise_without_unit.xlsx",row.names = F)

############## Case 2 Breaking Data PMCAT wise without unit consideration###################
data1<-data
# Importing MCAT parent id mapping sheet
pmcat_list<-read_excel("F:/Indiamart/Pricing Analytics/MCAT_PMCAT_17 June (2).xlsx")
# Merging base data with parent id data
data1<-sqldf("Select t1.*,t2.parentid from data1 t1 left join pmcat_list t2 on t1.FK_GLCAT_MCAT_ID=T2.CHILDID")

# Now after joining the products data got increased to 1,68,54711
# We have total 5747 parent ids
length(unique(data1$PARENTID))
# Breaking data PARENTID wise
subcat_break <- split( data1 , f = data1$PARENTID)
# Creating empty dataframe to store calculations of each PMCAT
stats2<-data.frame(ParentId=numeric(),
                   min=numeric(),
                   max=numeric(),
                   mean=numeric(),
                   median=numeric(),
                   mode=numeric(),
                   Prod_Cnt=numeric(),
                   Q1=numeric(),
                   Q3=numeric(),
                   IQR=numeric(),
                   MC=numeric(),
                   lower=numeric(),
                   upper=numeric(),
                   outlier_cnt=numeric(),
                   outlier_per=numeric())
i<-2
total_products<-0
for(i in 1:length(subcat_break))
{
# Getting each pmcat data
   a<-subcat_break[[i]]
   total_products<-total_products+length(a$PC_ITEM_FOB_PRICE)
   length_price<-length(subcat_break[[i]]$PC_ITEM_NAME)
   if(length_price)
   {
      #Store Price Value
      Price<-subcat_break[[i]]$PC_ITEM_FOB_PRICE
	  # Convert prices to numeric if character any
      Price<-as.numeric(subcat_break[[i]]$PC_ITEM_FOB_PRICE)
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
	  # Store Subcat data in data frame named a
      a<-subcat_break[[i]]
      #b<-a[,c(2,4,9,10,13)]
      a$PC_ITEM_FOB_PRICE<-as.numeric(a$PC_ITEM_FOB_PRICE)
	  # Apply completeFun as created above for column Price
      a<-completeFun(a, "PC_ITEM_FOB_PRICE")
      a$PC_ITEM_FOB_PRICE<-round(a$PC_ITEM_FOB_PRICE)
      #a$PC_ITEM_FOB_PRICE<-trimws(a$PC_ITEM_FOB_PRICE)
	  # Reindexing of rows
      rownames(a) <- 1:nrow(a)
	  # Apply my_populate function to assign outlier flag to observation
      a$check<-apply(a,1,function(params)my_populate(as.numeric(params[7]),lower,upper))
      #rownames(b) <- 1:nrow(b)
      #length_rem<-nrow(b)
	  # If Lower value as calculated by Adjusted Box Plot is negative, remove lower two percentile values and assign lowest value to lower value
      if(lower<0)
      {
         a$PC_ITEM_FOB_PRICE<-as.numeric(a$PC_ITEM_FOB_PRICE)
         lower_data<-min(a[a$PC_ITEM_FOB_PRICE>quantile(a$PC_ITEM_FOB_PRICE,0.02),c(7)])
         lower<-lower_data
      }
	  # Check outlier count and its percentage per subcat
      count_outliers<-sum(a$check==0)
      outliers_per<-(count_outliers/nrow(a))*100
	  # Appending values to empty dataframe as created above
      stats2<-rbind(stats2,data.frame(ParentId=unique(a$PARENTID),min=min(a$PC_ITEM_FOB_PRICE,na.rm = T),
                                      max=max(a$PC_ITEM_FOB_PRICE,na.rm = T),mean=mean(a$PC_ITEM_FOB_PRICE,na.rm = T),median=median(a$PC_ITEM_FOB_PRICE,na.rm = T),
                                      mode=getmode(a$PC_ITEM_FOB_PRICE),Prod_Cnt=nrow(a),Q1=Q1,Q3=Q3,IQR=IQR,MC=MC,lower=lower,upper=upper,outlier_cnt=count_outliers,outlier_per=outliers_per))
      
   }
   
}

length(sum(stats1$Prod_Cnt))    
Writing Final Output 
write.xlsx(stats2,"F:/Indiamart/Pricing Analytics/Pricing/Funnel/New Funnel/PMcatWise_without_unit.xlsx",row.names = F)

####################### Case 4 Breaking Data MCAT Wise without unit #####################################

data1<-data

length(unique(data1$FK_GLCAT_MCAT_ID))
# Splitting data MCAT ID wise
subcat_break <- split( data1 , f = data1$FK_GLCAT_MCAT_ID)
# Creatiing empty dataframe
stats4<-data.frame(MCATId=numeric(),
                   min=numeric(),
                   max=numeric(),
                   mean=numeric(),
                   median=numeric(),
                   mode=numeric(),
                   Prod_Cnt=numeric(),
                   Q1=numeric(),
                   Q3=numeric(),
                   IQR=numeric(),
                   MC=numeric(),
                   lower=numeric(),
                   upper=numeric(),
                   outlier_cnt=numeric(),
                   outlier_per=numeric())
i<-1
total_products<-0
for(i in 1:length(subcat_break))
{
   a<-subcat_break[[i]]
   # Assign total product count 
   total_products<-total_products+length(a$PC_ITEM_FOB_PRICE)
   # Check if there products exists for each mcat
   length_price<-length(subcat_break[[i]]$PC_ITEM_NAME)
   if(length_price)
   {
      #Store Price Value
      Price<-subcat_break[[i]]$PC_ITEM_FOB_PRICE
	  # Convert prices to numeric if character any
      Price<-as.numeric(subcat_break[[i]]$PC_ITEM_FOB_PRICE)
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
	  # Store mcat data in data frame named a
      a<-subcat_break[[i]]
      a$PC_ITEM_FOB_PRICE<-as.numeric(a$PC_ITEM_FOB_PRICE)
	  # Apply completeFun as created above for column Price
      a<-completeFun(a, "PC_ITEM_FOB_PRICE")
      a$PC_ITEM_FOB_PRICE<-round(a$PC_ITEM_FOB_PRICE)
      if(nrow(a)!=0)
      {
         if(!(is.na(lower) && is.na(upper)))
         {
		 # Reindexing of rows
            rownames(a) <- 1:nrow(a)
            # Apply my_populate function to assign outlier flag to observation
               a$check<-apply(a,1,function(params)my_populate(as.numeric(params[7]),lower,upper))
               
            
            # If Lower value as calculated by Adjusted Box Plot is negative, remove lower two percentile values and assign #lowest value to lower value
            if(lower<0)
            {
               a$PC_ITEM_FOB_PRICE<-as.numeric(a$PC_ITEM_FOB_PRICE)
               lower_data<-min(a[a$PC_ITEM_FOB_PRICE>quantile(a$PC_ITEM_FOB_PRICE,0.02),c(7)])
               lower<-lower_data
            }
			# Check outlier count and its percentage per subcat
            count_outliers<-sum(a$check==0)
            outliers_per<-(count_outliers/nrow(a))*100
			# Appending values to empty dataframe as created above
            stats4<-rbind(stats4,data.frame(MCATId=unique(a$FK_GLCAT_MCAT_ID),min=min(a$PC_ITEM_FOB_PRICE,na.rm = T),
                                            max=max(a$PC_ITEM_FOB_PRICE,na.rm = T),mean=mean(a$PC_ITEM_FOB_PRICE,na.rm = T),median=median(a$PC_ITEM_FOB_PRICE,na.rm = T),
                                            mode=getmode(a$PC_ITEM_FOB_PRICE),Prod_Cnt=nrow(a),Q1=Q1,Q3=Q3,IQR=IQR,MC=MC,lower=lower,upper=upper,outlier_cnt=count_outliers,outlier_per=outliers_per))
         }
      }
      
      
   }
   
}

   
# Writing Final Output
write.csv(stats4,"F:/Indiamart/Pricing Analytics/Pricing/Funnel/New Funnel/McatWise_without_unit.csv",row.names = F)

 #Removing objects not required
rm(a,data,data1,final_units,meshquery,meshrquery1,noofOutliersgreaterthan10,pmcat_list,subcat_break,unit_break,with_na,without_na)
###################################### Inclusive of Unit Consideration##########################################
rm(without_na_prod)
###############Case 3 Subcat Wise with unit consideration #############################
data1<-data
# Removing those units where MOQ_Unit_Types is null
data1<-data1[!is.na(data1$PC_ITEM_MOQ_UNIT_TYPE),]
length(unique(data1$FK_GLCAT_CAT_ID))
# Replacing Corr_units with MOQ_unit_types where Corr_Units is blank/null
with_na<-data1[is.na(data1$Corr_unit),]
without_na<-data1[!is.na(data1$Corr_unit),]
with_na$Corr_unit<-with_na$PC_ITEM_MOQ_UNIT_TYPE
data1<-rbind(with_na,without_na)

#3 Breaking Data Subcat Wise
subcat_break <- split( data1 , f = data1$FK_GLCAT_CAT_ID)
# Create an empty Data frame
stats3<-data.frame(SubCatID=numeric(),
                   unit=character(),
                   min=numeric(),
                   max=numeric(),
                   mean=numeric(),
                   median=numeric(),
                   mode=numeric(),
                   Prod_Cnt=numeric(),
                   Q1=numeric(),
                   Q3=numeric(),
                   IQR=numeric(),
                   MC=numeric(),
                   lower=numeric(),
                   upper=numeric(),
                   outlier_cnt=numeric(),
                   outlier_per=numeric())
i<-2
for(i in 1:length(subcat_break))
{
   a<-subcat_break[[i]]
   # Breaking data unit wise per subcat
   unit_break<-split(subcat_break[[i]],f=subcat_break[[i]]$Corr_unit)
   k<-2
   for(k in 1:length(unit_break))
   {
    # Proceed only if products per unit per subcat is greater than 10
      if(nrow(unit_break[[k]])>10)
      {
         length_price<-length(unit_break[[k]]$PC_ITEM_NAME)
         if(length_price)
         {
            #Store Price Value
            Price<-unit_break[[k]]$PC_ITEM_FOB_PRICE
            Price<-as.numeric(unit_break[[k]]$PC_ITEM_FOB_PRICE)
            Price[is.na(Price)]<-0
			# Sort the price
            Price<-sort(Price)
            #Remove zero prices
            Price<-Price[Price>0]
			# Calculate MC, Q1,Q3,IQR
            if(length(Price)!=0)
            {
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
               
               a<-unit_break[[k]]
               #b<-a[,c(2,4,9,10,13)]
               a$PC_ITEM_FOB_PRICE<-as.numeric(a$PC_ITEM_FOB_PRICE)
               a<-completeFun(a, "PC_ITEM_FOB_PRICE")
               a$PC_ITEM_FOB_PRICE<-round(a$PC_ITEM_FOB_PRICE)
               #a$PC_ITEM_FOB_PRICE<-trimws(a$PC_ITEM_FOB_PRICE)
               rownames(a) <- 1:nrow(a)
               a$check<-apply(a,1,function(params)my_populate(as.numeric(params[7]),lower,upper))
               #rownames(b) <- 1:nrow(b)
               #length_rem<-nrow(b)
               if(lower<0)
               {
                  a$PC_ITEM_FOB_PRICE<-as.numeric(a$PC_ITEM_FOB_PRICE)
                  lower_data<-min(a[a$PC_ITEM_FOB_PRICE>quantile(a$PC_ITEM_FOB_PRICE,0.02),c(7)])
                  lower<-lower_data
               }
               count_outliers<-sum(a$check==0)
               outliers_per<-(count_outliers/nrow(a))*100
               stats3<-rbind(stats3,data.frame(SubCatID=unique(a$FK_GLCAT_CAT_ID),unit=unique(a$Corr_unit),min=min(a$PC_ITEM_FOB_PRICE,na.rm = T),
                                               max=max(a$PC_ITEM_FOB_PRICE,na.rm = T),mean=mean(a$PC_ITEM_FOB_PRICE,na.rm = T),median=median(a$PC_ITEM_FOB_PRICE,na.rm = T),
                                               mode=getmode(a$PC_ITEM_FOB_PRICE),Prod_Cnt=nrow(a),Q1=Q1,Q3=Q3,IQR=IQR,MC=MC,lower=lower,upper=upper,outlier_cnt=count_outliers,outlier_per=outliers_per))
            }
         }
      }
   }
   
}

write.csv(stats3,"F:/Indiamart/Pricing Analytics/Pricing/Funnel/New Funnel/SubcatWise_with_units.csv",row.names = F)

############## Case 5 Breaking Data PMCAT wise with unit consideration###################
data1<-data
# Importing MCAT Parent ID Data
pmcat_list<-read_excel("F:/Indiamart/Pricing Analytics/MCAT_PMCAT_17 June (2).xlsx")
 # Merging Parent Id Data with base data
data1<-sqldf("Select t1.*,t2.parentid from data1 t1 left join pmcat_list t2 on t1.FK_GLCAT_MCAT_ID=T2.CHILDID")

# Now after joining the products data got increased to 1,68,54711
# Removing those units where MOQ_Unit_Types is null
data1<-data1[!is.na(data1$PC_ITEM_MOQ_UNIT_TYPE),]
# Replacing Corr_units with MOQ_unit_types where Corr_Units is blank/null
# Subsetting data where pmcat is not NA
data1<-data1[!is.na(data1$PARENTID),]

with_na<-data1[is.na(data1$Corr_unit),]
without_na<-data1[!is.na(data1$Corr_unit),]
with_na$Corr_unit<-with_na$PC_ITEM_MOQ_UNIT_TYPE
data1<-rbind(with_na,without_na)


# We have total 5747 parent ids
length(unique(data1$PARENTID))
# Breaking Data ParentId wise
subcat_break <- split( data1 , f = data1$PARENTID)

stats5<-data.frame(PMCATID=numeric(),
                   unit=character(),
                   min=numeric(),
                   max=numeric(),
                   mean=numeric(),
                   median=numeric(),
                   mode=numeric(),
                   Prod_Cnt=numeric(),
                   Q1=numeric(),
                   Q3=numeric(),
                   IQR=numeric(),
                   MC=numeric(),
                   lower=numeric(),
                   upper=numeric(),
                   outlier_cnt=numeric(),
                   outlier_per=numeric())
i<-1
for(i in 2:length(subcat_break))
{
   a<-subcat_break[[i]]
   # Breaking Data per unit per Parent Id
   unit_break<-split(subcat_break[[i]],f=subcat_break[[i]]$Corr_unit)
   k<-12
   for(k in 1:length(unit_break))
   { # Proceed if no. Of products per unit per parent id is greater than 10
      if(nrow(unit_break[[k]])>10)
      {
         length_price<-length(unit_break[[k]]$PC_ITEM_NAME)
         if(length_price)
         {
            #Store Price Value
            Price<-unit_break[[k]]$PC_ITEM_FOB_PRICE
            Price<-as.numeric(unit_break[[k]]$PC_ITEM_FOB_PRICE)
            Price[is.na(Price)]<-0
            Price<-sort(Price)
            #Remove zero prices
            Price<-Price[Price>0]
			 # Calculation of Q1, Q3, IQR, MC, lower cap and upper cap
            if(length(Price)!=0)
            {
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
               
               a<-unit_break[[k]]
               
               a$PC_ITEM_FOB_PRICE<-as.numeric(a$PC_ITEM_FOB_PRICE)
               a<-completeFun(a, "PC_ITEM_FOB_PRICE")
               a$PC_ITEM_FOB_PRICE<-round(a$PC_ITEM_FOB_PRICE)
               
               if(nrow(a)!=0)
               {
                  if(!(is.na(lower) && is.na(upper)))
                  {
                     rownames(a) <- 1:nrow(a)
                     a$check<-apply(a,1,function(params)my_populate(as.numeric(params[7]),lower,upper))
                     #rownames(b) <- 1:nrow(b)
                     #length_rem<-nrow(b)
                     if(lower<0)
                     {
                        a$PC_ITEM_FOB_PRICE<-as.numeric(a$PC_ITEM_FOB_PRICE)
                        lower_data<-min(a[a$PC_ITEM_FOB_PRICE>quantile(a$PC_ITEM_FOB_PRICE,0.02),c(7)])
                        lower<-lower_data
                     }
                     count_outliers<-sum(a$check==0)
                     outliers_per<-(count_outliers/nrow(a))*100
                     stats5<-rbind(stats5,data.frame(PMCATID=unique(a$PARENTID),unit=unique(a$Corr_unit),min=min(a$PC_ITEM_FOB_PRICE,na.rm = T),
                                                     max=max(a$PC_ITEM_FOB_PRICE,na.rm = T),mean=mean(a$PC_ITEM_FOB_PRICE,na.rm = T),median=median(a$PC_ITEM_FOB_PRICE,na.rm = T),
                                                     mode=getmode(a$PC_ITEM_FOB_PRICE),Prod_Cnt=nrow(a),Q1=Q1,Q3=Q3,IQR=IQR,MC=MC,lower=lower,upper=upper,outlier_cnt=count_outliers,outlier_per=outliers_per))
                        }
               }
            }
         }
      }
   }
   
}


length(sum(stats1$Prod_Cnt))    
# Writing Final Output
write.csv(stats5,"F:/Indiamart/Pricing Analytics/Pricing/Funnel/New Funnel/PMcatWise_with_unit.csv",row.names = F)


############## Case 5 Breaking Data MCAT wise with unit consideration###################
data1<-data
# Now after joining the products data got increased to 1,68,54711
# Removing those units where MOQ_Unit_Types is null
data1<-data1[!is.na(data1$PC_ITEM_MOQ_UNIT_TYPE),]
# Replacing Corr_units with MOQ_unit_types where Corr_Units is blank/null
# Subsetting data where pmcat is not NA
length(unique(data1$FK_GLCAT_MCAT_ID))
with_na<-data1[is.na(data1$Corr_unit),]
without_na<-data1[!is.na(data1$Corr_unit),]
with_na$Corr_unit<-with_na$PC_ITEM_MOQ_UNIT_TYPE
data1<-rbind(with_na,without_na)



length(unique(data1$FK_GLCAT_MCAT_ID))
 # Breaking data MCAT id wise
subcat_break <- split( data1 , f = data1$FK_GLCAT_MCAT_ID)

stats6<-data.frame(MCATID=numeric(),
                   unit=character(),
                   min=numeric(),
                   max=numeric(),
                   mean=numeric(),
                   median=numeric(),
                   mode=numeric(),
                   Prod_Cnt=numeric(),
                   Q1=numeric(),
                   Q3=numeric(),
                   IQR=numeric(),
                   MC=numeric(),
                   lower=numeric(),
                   upper=numeric(),
                   outlier_cnt=numeric(),
                   outlier_per=numeric())
i<-1
for(i in 1:length(subcat_break))
{
   a<-subcat_break[[i]]
    # Breaking Data per unit per MCat wise
   unit_break<-split(subcat_break[[i]],f=subcat_break[[i]]$Corr_unit)
   k<-12
   for(k in 1:length(unit_break))
   {
      if(nrow(unit_break[[k]])>10)
      {
         length_price<-length(unit_break[[k]]$PC_ITEM_NAME)
         if(length_price)
         {
            #Store Price Value
            Price<-unit_break[[k]]$PC_ITEM_FOB_PRICE
            Price<-as.numeric(unit_break[[k]]$PC_ITEM_FOB_PRICE)
            Price[is.na(Price)]<-0
            Price<-sort(Price)
            #Remove zero prices
            Price<-Price[Price>0]
			 # Calculation of metrics like Q1, Q3, IQR, MC, lower cap and upper cap
            if(length(Price)!=0)
            {
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
               
               a<-unit_break[[k]]
               #b<-a[,c(2,4,9,10,13)]
               a$PC_ITEM_FOB_PRICE<-as.numeric(a$PC_ITEM_FOB_PRICE)
               a<-completeFun(a, "PC_ITEM_FOB_PRICE")
               a$PC_ITEM_FOB_PRICE<-round(a$PC_ITEM_FOB_PRICE)
               #a$PC_ITEM_FOB_PRICE<-trimws(a$PC_ITEM_FOB_PRICE)
               if(nrow(a)!=0)
               {
                  if(!(is.na(lower) && is.na(upper)))
                  {
                     rownames(a) <- 1:nrow(a)
                     a$check<-apply(a,1,function(params)my_populate(as.numeric(params[7]),lower,upper))
                     #rownames(b) <- 1:nrow(b)
                     #length_rem<-nrow(b)
                     if(lower<0)
                     {
                        a$PC_ITEM_FOB_PRICE<-as.numeric(a$PC_ITEM_FOB_PRICE)
                        lower_data<-min(a[a$PC_ITEM_FOB_PRICE>quantile(a$PC_ITEM_FOB_PRICE,0.02),c(7)])
                        lower<-lower_data
                     }
                     count_outliers<-sum(a$check==0)
                     outliers_per<-(count_outliers/nrow(a))*100
                     stats6<-rbind(stats6,data.frame(MCATID=unique(a$FK_GLCAT_MCAT_ID),unit=unique(a$Corr_unit),min=min(a$PC_ITEM_FOB_PRICE,na.rm = T),
                                                     max=max(a$PC_ITEM_FOB_PRICE,na.rm = T),mean=mean(a$PC_ITEM_FOB_PRICE,na.rm = T),median=median(a$PC_ITEM_FOB_PRICE,na.rm = T),
                                                     mode=getmode(a$PC_ITEM_FOB_PRICE),Prod_Cnt=nrow(a),Q1=Q1,Q3=Q3,IQR=IQR,MC=MC,lower=lower,upper=upper,outlier_cnt=count_outliers,outlier_per=outliers_per))
                  }
               }
            }
         }
      }
   }
   
}

#####################Combining the MCAT wise final output data with corresponding Parent Id Data#################

# Importing MCAT final Data
mcat_with_units<-read.csv("F:/Indiamart/Pricing Analytics/Pricing/Funnel/New Funnel/McatWise_with_unit.csv")
# Import Parent Id Data 
mcat_pmcat_mapping<-read_excel("F:/Indiamart/Pricing Analytics/MCAT_PMCAT_17 June (2).xlsx")
#mcat_pmcat_mapping<-mcat_pmcat_mapping[order(mcat_pmcat_mapping$FK_CHILD_MCAT_ID),]

# Joining MCAt data with parent Id
dataofmcatwithpmcat<-sqldf("select a.*,b.PARENTID from mcat_with_units a left join  mcat_pmcat_mapping b on a.MCATID=b.CHILDID")
with_na_parent_mcat<-dataofmcatwithpmcat[is.na(dataofmcatwithpmcat$PARENTID),]
without_na_parent_mcat<-dataofmcatwithpmcat[!is.na(dataofmcatwithpmcat$PARENTID),]
length(unique(with_na_mcat$MCATID))

# Rearranging Columns  
dataofmcatwithpmcat<-dataofmcatwithpmcat[,c(1,17,2:16)]
pmcat_with_units<-read.csv("F:/Indiamart/Pricing Analytics/Pricing/Funnel/New Funnel/PMcatWise_with_unit.csv")
check<-sqldf("select a.*,b.* from dataofmcatwithpmcat a left join  pmcat_with_units b on a.PARENTID=b.PMCATID and a.unit=b.unit")
#check<-check[,-c(18)]
names(check)
# Renaming Columns
check<-rename(check,unit_pmcat=unit..19)
check<-rename(check,min_pmcat=min..20)
check<-rename(check,max_pmcat=max..21)
check<-rename(check,mean_pmcat=mean..22)
check<-rename(check,median_pmcat=median..23)
check<-rename(check,mode_pmcat=mode..24)
check<-rename(check,prod_cnt_pmcat=Prod_Cnt..25)
check<-rename(check,Q1_pmcat=Q1..26)
check<-rename(check,Q3_pmcat=Q3..27)
check<-rename(check,IQR_pmcat=IQR..28)
check<-rename(check,MC_pmcat=MC..29)
check<-rename(check,lower_pmcat=lower..30)
check<-rename(check,upper_pmcat=upper..31)
check<-rename(check,outlier_cnt_pmcat=outlier_cnt..32)
check<-rename(check,outlier_per_pmcat=outlier_per..33)

 # Code to assign final lower and upper after log consideration
logcheck<-function(x,y)
{
   if(!(is.na(x))&!(is.na(y)))
   {
   if(x!=1 & y!=1)
   {
   if(x!=0 & y!=0)
   {
      if(log(y)/log(x)>=1.2)
      {
         return(x)
      }
      else
      {
         return(y)
      }
   }
      if(x==0 & y!=0)
      {
        
            return(y)
      }
   }
   }
}

logcheck<-function(x,y)
{
   if(!(is.na(x))&!(is.na(y)))
   {
         if(x!=1 & y!=1)
         {
            if(x!=0 & y!=0)
            {
               if(log(y)/log(x)>=1.2)
               {
                  return(x)
               }
               else
               {
                  return(y)
               }
            }
            if(x==0 & y!=0)
            {
               
               return(y)
            }
            if(y==0 & x!=0)
            {
               
               return(x)
            }
         }
      if(x!=1 & y==1)
      {
         return(y)
      }
      if(x==1 & y!=1)
      {
         return(x)
      }
   }
}


#if(log(1)/log(1)==-Inf)
chechme<-check[1,]

check$final_mcat_max<-apply(check,1,function(params)logcheck(as.numeric(params[5]),as.numeric(params[15])))
check$final_mcat_min<-apply(check,1,function(params)logcheck(as.numeric(params[4]),as.numeric(params[14])))

check$final_pmcat_max<-apply(check,1,function(params)logcheck(as.numeric(params[21]),as.numeric(params[31])))
check$final_pmcat_min<-apply(check,1,function(params)logcheck(as.numeric(params[20]),as.numeric(params[30])))

checkme<-check
checkme$final_mcat_max <- vapply(checkme$final_mcat_max , paste, collapse = ", ", character(1L))
checkme$final_mcat_min <- vapply(checkme$final_mcat_min  , paste, collapse = ", ", character(1L))
checkme$final_pmcat_max <- vapply(checkme$final_pmcat_max , paste, collapse = ", ", character(1L))
checkme$final_pmcat_min <- vapply(checkme$final_pmcat_min, paste, collapse = ", ", character(1L))
write.csv(checkme,"F:/Indiamart/Pricing Analytics/Pricing/Funnel/New Funnel/McatWise_pmcat_final.csv",row.names = F)

 
# Writing Final Output
write.csv(stats6,"F:/Indiamart/Pricing Analytics/Pricing/Funnel/New Funnel/McatWise_with_unit.csv",row.names = F)


