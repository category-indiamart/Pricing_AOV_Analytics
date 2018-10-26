#load Data
load("C:/Ankit Verma/Old Laptop/Ankit Backup/Pear work/Shivendra/Pricing Analytics/SubCatData.RData")
rm(jdbcConMesh,jdbcDriver,id,query1)
# Converting Units to lowercase and trimming
meshrquery1$PC_ITEM_MOQ_UNIT_TYPE<-tolower(meshrquery1$PC_ITEM_MOQ_UNIT_TYPE)
meshrquery1$PC_ITEM_MOQ_UNIT_TYPE<-trimws(meshrquery1$PC_ITEM_MOQ_UNIT_TYPE,which = c("both", "left", "right"))

# Getting the distinct unit types along with their frequencies
query<-"select distinct PC_ITEM_MOQ_UNIT_TYPE,COUNT(PC_ITEM_MOQ_UNIT_TYPE) FROM meshrquery1 group by PC_ITEM_MOQ_UNIT_TYPE"
data<-sqldf(query)
write.xlsx(data,"C:/Ankit Verma/Old Laptop/Ankit Backup/Pear work/Shivendra/Pricing Analytics/check.xlsx")
write.csv(data,"C:/Ankit Verma/Old Laptop/Ankit Backup/Pear work/Shivendra/Pricing Analytics/units_final.csv")

# Importing the unit file so that it can be merged
final_units<-read_excel("C:/Users/imart/Downloads/FINAL UNIT.xlsx",sheet=1)
final_units<-head(final_units,n=32706)
names(final_units)
final_units<-rename(final_units,tot_cnt=`COUNT(PC_ITEM_MOQ_UNIT_TYPE)`)
final_units<-rename(final_units,contr=`% Of Product`)
final_units<-rename(final_units,Corr_unit=`Final Unit`)

# Converting Unit types to lowercase and trimming so that correctly merged
final_units$PC_ITEM_MOQ_UNIT_TYPE<-tolower(final_units$PC_ITEM_MOQ_UNIT_TYPE)
final_units$PC_ITEM_MOQ_UNIT_TYPE<-trimws(final_units$PC_ITEM_MOQ_UNIT_TYPE,which = c("both", "left", "right"))

data<- sqldf("Select t1.*,t2.Corr_Unit from meshrquery1 t1 join final_units t2 on t1.PC_ITEM_MOQ_UNIT_TYPE=t2.PC_ITEM_MOQ_UNIT_TYPE")
length(unique(data$PC_ITEM_MOQ_UNIT_TYPE))


subcat_break <- split( data , f = data$FK_GLCAT_CAT_ID)
stats<-data.frame(SubCatID=numeric(),
                  PMCATID=numeric(),
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
for(i in 1:length(subcat_break))
{
  a<-subcat_break[[i]]
  if(nrow(a)>1000)
  {
    pmcat_break<-split(subcat_break[[i]],f=subcat_break[[i]]$FK_GLCAT_MCAT_ID)
    
    j<-1
    for(j in 1:length(pmcat_break))
    {
      unit_break<-split(pmcat_break[[j]],f=pmcat_break[[j]]$Corr_unit)
      k<-3
      for(k in 1:length(unit_break))
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
          count_outliers<-sum(a$check==0)
          outliers_per<-(count_outliers/nrow(a))*100
          stats<-rbind(stats,data.frame(SubCatID=unique(a$FK_GLCAT_CAT_ID),PMCATID=unique(a$FK_GLCAT_MCAT_ID),Unit=unique(a$Corr_unit),
                                        Prod_Cnt=nrow(a),Q1=Q1,Q3=Q3,IQR=IQR,MC=MC,lower=lower,upper=upper,outlier_cnt=count_outliers,outlier_per=outliers_per))
        }
      }
      
    }
  }
  
}

write.xlsx(stats,"C:/Ankit Verma/Old Laptop/Ankit Backup/Pear work/Shivendra/Pricing Analytics/Final_Data.xlsx",row.names = F)

