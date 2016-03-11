library(DT)
library(dplyr)
setwd("/Users/jack/CHP2/R_2016_01_15_13_52_13_user_S5-00391-3-Cancer_Hotspot_Panel_v2.vcf")

data <- read.csv(file = "TSVC_variants_IonCode_0101_new.myanno.hg19_multianno.csv",header = TRUE)

#data %>%
#  select(Chr,Start,End,Ref,Alt,Func.refGene,Gene.refGene,ExonicFunc.refGene,clinvar_20150629)

remove_column <- names(data) %in% c ("AAChange.refGene","CLNACC","CLNDSDBID","genomicSuperDups","SIFT_pred","Polyphen2_HDIV_pred","Polyphen2_HVAR_pred","LRT_pred","MutationTaster_pred","MutationAssessor_pred","FATHMM_pred","RadialSVM_pred","LR_pred","CADD_phred","")
data[!remove_column]
