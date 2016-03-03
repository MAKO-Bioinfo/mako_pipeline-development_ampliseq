library(DT)
library(dplyr)

data <- read.csv(file = "/Users/jack/CHP2/R_2016_01_15_13_52_13_user_S5-00391-3-Cancer_Hotspot_Panel_v2.vcf/TSVC_variants_IonCode_0101.myanno.hg19_multianno.csv",
header = TRUE)

data %>%
  select(Chr,Start,End,Ref,Alt,Func.refGene,Gene.refGene,ExonicFunc.refGene,clinvar_20150629)


