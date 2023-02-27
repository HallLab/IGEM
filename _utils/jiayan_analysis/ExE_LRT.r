# Here is an example of utilizing the outputs from IGEM to perform the ExE in R with LRTs in the loop. (PLATO cannot take E data to perform interactions, so I wrote such a script in R). 

# Code to map description back to NHANES IDs


# Read in the tables
Normalization <- read.csv("/users/andrerico/dev/lab/jiayan_ctd/Normalization.csv",na.strings=c("","NA"))
keylink <- read.csv("/users/andrerico/dev/lab/jiayan_ctd/keylink.csv")
# VarDescription <- read.csv("~/Desktop/IGEM/S2/VarDescription.csv")

# Process to find pairs
pairID = keylink[,c(5,9)]
normalization_short = Normalization[,c(2,3)]
for (i in c("anat","go","path","meta:hmdb0002111")) {
  normalization_short = normalization_short[!grepl(i, normalization_short$keyge),]
}

completeFun <- function(data, desiredCols) {
  completeVec <- complete.cases(data[, desiredCols])
  return(data[completeVec, ])
}

normalization_clean = completeFun(normalization_short, "keyge")
normalization_clean = normalization_clean[!duplicated(normalization_clean$Fatores), ]

pairMap = merge(pairID,normalization_clean,by.x="keyge_1",by.y="keyge")
colnames(pairMap)[3] = "Desc_1"
pairMap2 = merge(pairMap,normalization_clean,by.x="keyge_2",by.y="keyge")
colnames(pairMap2)[4] = "Desc_2"
​
VarDesc_Short = VarDescription[,c(5,6)]
VarDesc_clean = VarDesc_Short[!duplicated(VarDesc_Short$var_desc), ]
​
pairMap2$Desc_1 = toupper(pairMap2$Desc_1) 
pairMap2$Desc_2 = toupper(pairMap2$Desc_2) 
VarDesc_clean$var_desc = toupper(VarDesc_clean$var_desc) 
ToNAHNESID = merge(pairMap2,VarDesc_clean,by.x="Desc_1",by.y="var_desc")
colnames(ToNAHNESID)[5] = "NHANESID_var1"
ToNAHNESID = merge(ToNAHNESID,VarDesc_clean,by.x="Desc_2",by.y="var_desc")
colnames(ToNAHNESID)[6] = "NHANESID_var2"
​
NAHNESID = ToNAHNESID[c(5,6)]
NAHNESID = NAHNESID[!duplicated(NAHNESID[c(1,2)]),]
​
write.table(NAHNESID, file="~/Desktop/IGEM/S2/gepairs.txt",quote = FALSE,row.names = FALSE)
​
# check Keylink: test pairs if all in NAHENS, such no disease
keylink_dup = keylink
for (i in c("anat","go","path","dise","gene","meta:hmdb0002111")) {
  keylink_dup = keylink_dup[!grepl(i, keylink_dup$keyge_1),]
  keylink_dup = keylink_dup[!grepl(i, keylink_dup$keyge_2),]
}


# - - - - - - - -  -- - - - - - - - - - - - - - - - - 

# Regression and LRTs with ExE pairs in R 
library(lmtest)
MainTable <- read.csv("~/Desktop/Anemia/Data/MainTable.csv")
gepairs <- read.csv("~/Desktop/IGEM/S2/gepairs.txt", sep="")
completeFun <- function(data, desiredCols) {
  completeVec <- complete.cases(data[, desiredCols])
  return(data[completeVec, ])
}
​
remove <- c('pneu', 'current_asthma',"ever","any","ATORVASTATIN","AZITHROMYCIN","CARVEDILOL","hepb","FENOFIBRATE","FLUOXETINE","BUPROPION","GLYBURIDE","ASPIRIN","heroin","ALENDRONATE","METFORMIN","ESTRADIOL","OMEPRAZOLE","NIFEDIPINE","PREDNISONE","PIOGLITAZONE","ROFECOXIB","ALBUTEROL","SPIRONOLACTONE","SIMVASTATIN","SERTRALINE","LOVASTATIN","LOSARTAN","cocaine","DIGOXIN","CELECOXIB")
​
#remove rows that contain any string in the vector in the team column
gepairs = gepairs[!grepl(paste(remove, collapse='|'), gepairs$NHANESID_var1),]
gepairs = gepairs[!grepl(paste(remove, collapse='|'), gepairs$NHANESID_var2),]
​
## HEMOGLOBIN
resultstable_dis=data.frame()
resultstable_rep=data.frame()
for (i in 1:nrow(gepairs)){
  e1=gepairs[i,1]
  e2=gepairs[i,2]
  nested_table=as.data.frame(MainTable[,c("LBXHGB","female", "black", "mexican", "other_hispanic", "other_eth", "SDDSRVYR", "BMXBMI", "SES_LEVEL", "RIDAGEYR", "LBXCOT", "IRON_mg",e1,e2)])
  nested_table[is.na(nested_table)] <- 0
  nested_table_dis = nested_table[nested_table$SDDSRVYR == "1"|nested_table$SDDSRVYR == "2",]
  complex_table=nested_table
  complex_table[is.na(complex_table)] <- 0
  complex_table$interaction=complex_table[,c(e1)]*complex_table[,c(e2)]
  complex_table_dis = complex_table[complex_table$SDDSRVYR == "1"|complex_table$SDDSRVYR == "2",]
​
  nested <- glm(LBXHGB~.,data=nested_table)
  complex <- glm(LBXHGB~.,data=complex_table)
  result=lrtest(nested,complex)
  resultstable_dis[i,1] = result[["Pr(>Chisq)"]][[2]]
  resultstable_dis[i,2] = e1
  resultstable_dis[i,3] = e2
} 
​
colnames(resultstable_dis) = c("p","e1","e2")
resultstable_dis$Bonfp = resultstable_dis$p * nrow(gepairs)
​
sign_dis_hemo = resultstable_dis[resultstable_dis$Bonfp < 0.05,]
​
for (i in 1:nrow(sign_dis_hemo)){
  e1=sign_dis_hemo[i,2]
  e2=sign_dis_hemo[i,3]
  nested_table=as.data.frame(MainTable[,c("LBXHGB","female", "black", "mexican", "other_hispanic", "other_eth", "SDDSRVYR", "BMXBMI", "SES_LEVEL", "RIDAGEYR", "LBXCOT", "IRON_mg",e1,e2)])
  nested_table[is.na(nested_table)] <- 0
  complex_table=nested_table
  complex_table[is.na(complex_table)] <- 0
  complex_table$interaction=complex_table[,c(e1)]*complex_table[,c(e2)]
​
  nested_table_rep = nested_table[nested_table$SDDSRVYR == "3"|nested_table$SDDSRVYR == "4",]
  complex_table_rep = complex_table[complex_table$SDDSRVYR == "3"|complex_table$SDDSRVYR == "4",]
  
  nested_rep <- glm(LBXHGB~.,data=nested_table_rep)
  complex_rep <- glm(LBXHGB~.,data=complex_table_rep)
  result_rep=lrtest(nested_rep,complex_rep)
  resultstable_rep[i,1] = result_rep[["Pr(>Chisq)"]][[2]]
  resultstable_rep[i,2] = e1
  resultstable_rep[i,3] = e2
} 
​
colnames(resultstable_rep) = c("p","e1","e2")
resultstable_rep$Bonfp = resultstable_rep$p * nrow(sign_dis_hemo)
​
sign_rep_hemo = resultstable_rep[resultstable_rep$Bonfp < 0.05,]
​
write.table(sign_dis_hemo, file="~/Desktop/IGEM/S2/sign_dis_hemo.txt",quote = FALSE,row.names = FALSE)
write.table(sign_rep_hemo, file="~/Desktop/IGEM/S2/sign_rep_hemo.txt",quote = FALSE,row.names = FALSE)
​
## VB12
resultstable_dis=data.frame()
resultstable_rep=data.frame()
for (i in 1:nrow(gepairs)){
  e1=gepairs[i,1]
  e2=gepairs[i,2]
  nested_table=as.data.frame(MainTable[,c("LBXB12","female", "black", "mexican", "other_hispanic", "other_eth", "SDDSRVYR", "BMXBMI", "SES_LEVEL", "RIDAGEYR", "LBXCOT", "IRON_mg",e1,e2)])
  nested_table[is.na(nested_table)] <- 0
  nested_table_dis = nested_table[nested_table$SDDSRVYR == "1"|nested_table$SDDSRVYR == "2",]
  complex_table=nested_table
  complex_table[is.na(complex_table)] <- 0
  complex_table$interaction=complex_table[,c(e1)]*complex_table[,c(e2)]
  complex_table_dis = complex_table[complex_table$SDDSRVYR == "1"|complex_table$SDDSRVYR == "2",]
  
  nested <- glm(LBXB12~.,data=nested_table)
  complex <- glm(LBXB12~.,data=complex_table)
  result=lrtest(nested,complex)
  resultstable_dis[i,1] = result[["Pr(>Chisq)"]][[2]]
  resultstable_dis[i,2] = e1
  resultstable_dis[i,3] = e2
} 
​
colnames(resultstable_dis) = c("p","e1","e2")
resultstable_dis$Bonfp = resultstable_dis$p * nrow(gepairs)
​
sign_dis_vb12 = resultstable_dis[resultstable_dis$Bonfp < 0.05,]
​
for (i in 1:nrow(sign_dis_vb12)){
  e1=sign_dis_vb12[i,2]
  e2=sign_dis_vb12[i,3]
  nested_table=as.data.frame(MainTable[,c("LBXB12","female", "black", "mexican", "other_hispanic", "other_eth", "SDDSRVYR", "BMXBMI", "SES_LEVEL", "RIDAGEYR", "LBXCOT", "IRON_mg",e1,e2)])
  nested_table[is.na(nested_table)] <- 0
  complex_table=nested_table
  complex_table[is.na(complex_table)] <- 0
  complex_table$interaction=complex_table[,c(e1)]*complex_table[,c(e2)]
  
  nested_table_rep = nested_table[nested_table$SDDSRVYR == "3"|nested_table$SDDSRVYR == "4",]
  complex_table_rep = complex_table[complex_table$SDDSRVYR == "3"|complex_table$SDDSRVYR == "4",]
  
  nested_rep <- glm(LBXB12~.,data=nested_table_rep)
  complex_rep <- glm(LBXB12~.,data=complex_table_rep)
  result_rep=lrtest(nested_rep,complex_rep)
  resultstable_rep[i,1] = result_rep[["Pr(>Chisq)"]][[2]]
  resultstable_rep[i,2] = e1
  resultstable_rep[i,3] = e2
} 
​
colnames(resultstable_rep) = c("p","e1","e2")
resultstable_rep$Bonfp = resultstable_rep$p * nrow(sign_dis_vb12)
​
sign_rep_vb12 = resultstable_rep[resultstable_rep$Bonfp < 0.05,]
​
write.table(sign_dis_vb12, file="~/Desktop/IGEM/S2/sign_dis_vb12.txt",quote = FALSE,row.names = FALSE)
write.table(sign_rep_vb12, file="~/Desktop/IGEM/S2/sign_rep_vb12.txt",quote = FALSE,row.names = FALSE)
​
​
## RBC FOLATE
resultstable_dis=data.frame()
resultstable_rep=data.frame()
for (i in 1:nrow(gepairs)){
  e1=gepairs[i,1]
  e2=gepairs[i,2]
  nested_table=as.data.frame(MainTable[,c("LBXRBF","female", "black", "mexican", "other_hispanic", "other_eth", "SDDSRVYR", "BMXBMI", "SES_LEVEL", "RIDAGEYR", "LBXCOT", "IRON_mg",e1,e2)])
  nested_table[is.na(nested_table)] <- 0
  nested_table_dis = nested_table[nested_table$SDDSRVYR == "1"|nested_table$SDDSRVYR == "2",]
  complex_table=nested_table
  complex_table[is.na(complex_table)] <- 0
  complex_table$interaction=complex_table[,c(e1)]*complex_table[,c(e2)]
  complex_table_dis = complex_table[complex_table$SDDSRVYR == "1"|complex_table$SDDSRVYR == "2",]
  
  nested <- glm(LBXRBF~.,data=nested_table)
  complex <- glm(LBXRBF~.,data=complex_table)
  result=lrtest(nested,complex)
  resultstable_dis[i,1] = result[["Pr(>Chisq)"]][[2]]
  resultstable_dis[i,2] = e1
  resultstable_dis[i,3] = e2
} 
​
colnames(resultstable_dis) = c("p","e1","e2")
resultstable_dis$Bonfp = resultstable_dis$p * nrow(gepairs)
​
sign_dis_rbcf = resultstable_dis[resultstable_dis$Bonfp < 0.05,]
​
for (i in 1:nrow(sign_dis_rbcf)){
  e1=sign_dis_rbcf[i,2]
  e2=sign_dis_rbcf[i,3]
  nested_table=as.data.frame(MainTable[,c("LBXRBF","female", "black", "mexican", "other_hispanic", "other_eth", "SDDSRVYR", "BMXBMI", "SES_LEVEL", "RIDAGEYR", "LBXCOT", "IRON_mg",e1,e2)])
  nested_table[is.na(nested_table)] <- 0
  complex_table=nested_table
  complex_table[is.na(complex_table)] <- 0
  complex_table$interaction=complex_table[,c(e1)]*complex_table[,c(e2)]
  
  nested_table_rep = nested_table[nested_table$SDDSRVYR == "3"|nested_table$SDDSRVYR == "4",]
  complex_table_rep = complex_table[complex_table$SDDSRVYR == "3"|complex_table$SDDSRVYR == "4",]
  
  nested_rep <- glm(LBXRBF~.,data=nested_table_rep)
  complex_rep <- glm(LBXRBF~.,data=complex_table_rep)
  result_rep=lrtest(nested_rep,complex_rep)
  resultstable_rep[i,1] = result_rep[["Pr(>Chisq)"]][[2]]
  resultstable_rep[i,2] = e1
  resultstable_rep[i,3] = e2
} 
​
colnames(resultstable_rep) = c("p","e1","e2")
resultstable_rep$Bonfp = resultstable_rep$p * nrow(sign_dis_rbcf)
​
sign_rep_rbcf = resultstable_rep[resultstable_rep$Bonfp < 0.05,]
​
write.table(sign_dis_rbcf, file="~/Desktop/IGEM/S2/sign_dis_rbcf.txt",quote = FALSE,row.names = FALSE)
write.table(sign_rep_rbcf, file="~/Desktop/IGEM/S2/sign_rep_rbcf.txt",quote = FALSE,row.names = FALSE)
​
​
## IRON
resultstable_dis=data.frame()
resultstable_rep=data.frame()
for (i in 1:nrow(gepairs)){
  e1=gepairs[i,1]
  e2=gepairs[i,2]
  nested_table=as.data.frame(MainTable[,c("LBXIRN","female", "black", "mexican", "other_hispanic", "other_eth", "SDDSRVYR", "BMXBMI", "SES_LEVEL", "RIDAGEYR", "LBXCOT", "IRON_mg",e1,e2)])
  nested_table[is.na(nested_table)] <- 0
  nested_table_dis = nested_table[nested_table$SDDSRVYR == "1"|nested_table$SDDSRVYR == "2",]
  complex_table=nested_table
  complex_table[is.na(complex_table)] <- 0
  complex_table$interaction=complex_table[,c(e1)]*complex_table[,c(e2)]
  complex_table_dis = complex_table[complex_table$SDDSRVYR == "1"|complex_table$SDDSRVYR == "2",]
  
  nested <- glm(LBXIRN~.,data=nested_table)
  complex <- glm(LBXIRN~.,data=complex_table)
  result=lrtest(nested,complex)
  resultstable_dis[i,1] = result[["Pr(>Chisq)"]][[2]]
  resultstable_dis[i,2] = e1
  resultstable_dis[i,3] = e2
} 
​
colnames(resultstable_dis) = c("p","e1","e2")
resultstable_dis$Bonfp = resultstable_dis$p * nrow(gepairs)
​
sign_dis_iron = resultstable_dis[resultstable_dis$Bonfp < 0.05,]
​
for (i in 1:nrow(sign_dis_iron)){
  e1=sign_dis_iron[i,2]
  e2=sign_dis_iron[i,3]
  nested_table=as.data.frame(MainTable[,c("LBXIRN","female", "black", "mexican", "other_hispanic", "other_eth", "SDDSRVYR", "BMXBMI", "SES_LEVEL", "RIDAGEYR", "LBXCOT", "IRON_mg",e1,e2)])
  nested_table[is.na(nested_table)] <- 0
  complex_table=nested_table
  complex_table[is.na(complex_table)] <- 0
  complex_table$interaction=complex_table[,c(e1)]*complex_table[,c(e2)]
  
  nested_table_rep = nested_table[nested_table$SDDSRVYR == "3"|nested_table$SDDSRVYR == "4",]
  complex_table_rep = complex_table[complex_table$SDDSRVYR == "3"|complex_table$SDDSRVYR == "4",]
  
  nested_rep <- glm(LBXIRN~.,data=nested_table_rep)
  complex_rep <- glm(LBXIRN~.,data=complex_table_rep)
  result_rep=lrtest(nested_rep,complex_rep)
  resultstable_rep[i,1] = result_rep[["Pr(>Chisq)"]][[2]]
  resultstable_rep[i,2] = e1
  resultstable_rep[i,3] = e2
} 
​
colnames(resultstable_rep) = c("p","e1","e2")
resultstable_rep$Bonfp = resultstable_rep$p * nrow(sign_dis_iron)
​
sign_rep_iron = resultstable_rep[resultstable_rep$Bonfp < 0.05,]
​
write.table(sign_dis_iron, file="~/Desktop/IGEM/S2/sign_dis_iron.txt",quote = FALSE,row.names = FALSE)
write.table(sign_rep_iron, file="~/Desktop/IGEM/S2/sign_rep_iron.txt",quote = FALSE,row.names = FALSE)

