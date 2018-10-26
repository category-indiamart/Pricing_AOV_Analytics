#DATASET 3
library(readxl)
library(discretization)
library(dplyr)
ls("package:discretization")
?mdlp
#ISQ Probability
#dfC <- read_excel("C:/Users/IMART/Downloads/o_c.xlsx")
dfC <- read.csv("E:/Pricing_analytics/final_AOV_data_city_paid_seller.csv")
#df1<-dfC[,c(8,2)]
df1 <- dfC[,c(2,4,10)]
df1$MCAT_CITY <- paste(df1$GLCAT_MCAT_NAME,df1$GL_CITY_NAME)
#df1$`PMCAT Name`<-as.factor(df1$`PMCAT Name`)
class(df1$GLCAT_MCAT_NAME)
class(df1$MCAT_CITY)
df1 <- df1[!is.na(df1$MCAT_CITY),c(4,3)]
df1<-as.data.frame(df1)
df1[is.na(df1)]<-0
#c1 <-mdlp(df1)$Disc.data
c1 <- mdlp(df1)$Disc.data
df1$'DiscretizedProbabiltyISQ' <- c1$Probability_ISQ
#df1<- df1[,c(2,1,3)]

df1 <- df1[,c(2,1)]

df_sold_ratio <- df1[,1]
df_sold_ratio <- as.data.frame(df_sold_ratio)
df_sold_ratio$Label <- "Label"
class(df_sold_ratio$Label)
df_sold_ratio$Label <- as.factor(df_sold_ratio$Label)
c4 <- mdlp(df_sold_ratio)$Disc.data
#write.xlsx(c1,"C:/Users/IMART/Downloads/isq2name2.xlsx")
dbS
#"Probability_ISQ","Probability_ISQ2","Probability_ISQ_Name","Probability_ISQ_Name2","Probability_MCAT","Probability_MCAT2","Probability_MCAT_Name","Probability_MCAT_Name2")]

#ISQ Name Probability
df2<-dfC[,c(10,2)]
df2$`PMCAT Name`<-as.factor(df2$`PMCAT Name`)
df2<-as.data.frame(df2)
df2[is.na(df2)]<-0
c2 <-mdlp(df2)$Disc.data
df2$'DiscretizedProbabiltyISQName' <- c2$Probability_ISQ_Name
df2<- df2[,c(2,1,3)]


#MCATProbability
df3<-dfC[,c(12,2)]
df3$`PMCAT Name`<-as.factor(df3$`PMCAT Name`)
df3<-as.data.frame(df3)
df3[is.na(df3)]<-0
c3 <-mdlp(df3)$Disc.data
df3$'DiscretizedProbabiltyPMCAT' <- c3$Probability_MCAT
df3<- df3[,c(2,1,3)]

#MCAT Name Probability
df4<-dfC[,c(14,2)]
df4$`PMCAT Name`<-as.factor(df4$`PMCAT Name`)
df4<-as.data.frame(df4)
df4[is.na(df4)]<-0
c4 <-mdlp(df4)$Disc.data
df4$'DiscretizedProbabiltyPMCAT_Name' <- c4$Probability_MCAT_Name
df4<- df4[,c(2,1,3)]

#Alt Probability
df5<-dfC[,c(16,2)]
df5$`PMCAT Name`<-as.factor(df5$`PMCAT Name`)
df5<-as.data.frame(df5)
df5[is.na(df5)]<-0
c5 <-mdlp(df5)$Disc.data
df5$'DiscretizedProbabiltyAlt' <- c5$Probability_Alt
df5<- df5[,c(2,1,3)]

#Alt Name Probability
df6<-dfC[,c(18,2)]
df6$`PMCAT Name`<-as.factor(df6$`PMCAT Name`)
df6<-as.data.frame(df6)
df6[is.na(df6)]<-0
c6 <-mdlp(df6)$Disc.data
df6$'DiscretizedProbabiltyAlt_Name' <- c6$Probability_Alt_Name
df6<- df6[,c(2,1,3)]

#Meta Probability
df7<-dfC[,c(20,2)]
df7$`PMCAT Name`<-as.factor(df7$`PMCAT Name`)
df7<-as.data.frame(df7)
df7[is.na(df7)]<-0
c7 <-mdlp(df7)$Disc.data
df7$'DiscretizedProbabiltyMeta' <- c7$Probability_Meta
df7<- df7[,c(2,1,3)]


#Meta Name Probability
df8<-dfC[,c(22,2)]
df8$`PMCAT Name`<-as.factor(df8$`PMCAT Name`)
df8<-as.data.frame(df8)
df8[is.na(df8)]<-0
c8 <-mdlp(df8)$Disc.data
df8$'DiscretizedProbabiltyMeta_Name' <- c8$Probability_Meta_Name
df8<- df8[,c(2,1,3)]


#FAQ Probability
df9<-dfC[,c(24,2)]
df9$`PMCAT Name`<-as.factor(df9$`PMCAT Name`)
df9<-as.data.frame(df9)
df9[is.na(df9)]<-0
c9 <-mdlp(df9)$Disc.data
df9$'DiscretizedProbabiltyFAQ' <- c9$Probability_FAQ
df9<- df9[,c(2,1,3)]

#FAQ Name Probability
df10<-dfC[,c(26,2)]
df10$`PMCAT Name`<-as.factor(df10$`PMCAT Name`)
df10<-as.data.frame(df10)
df10[is.na(df10)]<-0
c10 <-mdlp(df10)$Disc.data
df10$'DiscretizedProbabiltyFAQ_Name' <- c10$Probability_FAQ_Name
df10<- df10[,c(2,1,3)]

#City Probability
df11<-dfC[,c(28,2)]
df11$`PMCAT Name`<-as.factor(df11$`PMCAT Name`)
df11<-as.data.frame(df11)
df11[is.na(df11)]<-0
c11 <-mdlp(df11)$Disc.data
df11$'DiscretizedProbabiltyCity' <- c11$Probability_City
df11<- df11[,c(2,1,3)]


#City Name Probability
df12<-dfC[,c(30,2)]
df12$`PMCAT Name`<-as.factor(df12$`PMCAT Name`)
df12<-as.data.frame(df12)
df12[is.na(df12)]<-0
c12 <-mdlp(df12)$Disc.data
df12$'DiscretizedProbabiltyCity_Name' <- c12$Probability_City_Name
df12<- df11[,c(2,1,3)]

#Stopwords Probability
df13<-dfC[,c(32,2)]
df13$`PMCAT Name`<-as.factor(df13$`PMCAT Name`)
df13<-as.data.frame(df13)
df13[is.na(df13)]<-0
c13 <-mdlp(df13)$Disc.data
df13$'DiscretizedProbabiltyStopwords' <- c13$Probability_Stopwords
df13<- df13[,c(2,1,3)]

#Stopwords Name Probability
df14<-dfC[,c(34,2)]
df14$`PMCAT Name`<-as.factor(df14$`PMCAT Name`)
df14<-as.data.frame(df14)
df14[is.na(df14)]<-0
c14 <-mdlp(df14)$Disc.data
df14$'DiscretizedProbabiltyStopwords_Name' <- c14$Probability_Stopwords_Name
df14<- df14[,c(2,1,3)]

z <-cbind.data.frame(df1,df2,df3,df4,df5,df6,df7,df8,df9,df10,df11,df12,df13,df14)
z <-z[,unique(colnames(z))]
write.csv(z,"C:/Users/IMART/Downloads/discretized-o_c.csv",row.names = FALSE)