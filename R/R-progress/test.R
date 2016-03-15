setwd("/Users/jack/CHP2/R_2016_01_15_13_52_13_user_S5-00391-3-Cancer_Hotspot_Panel_v2.vcf")
dataframe <- read.csv("TSVC_variants_IonCode_0101_03042016.myanno.hg19_multianno.csv",header = T)
omimdb <- read.csv("omiminfo.csv",header = T)
#omiminfo <- subset(omimdb,select=c('Gene.refGene','GeneName'))


library(tidyr)
library(dplyr)
# omiminfo %>%
#   mutate(Gene.refGene = strsplit(as.character(Gene.refGene),",")) %>%
#   unnest(Gene.refGene) %>%
#   #merge(dataframe,omiminfo,by="Gene.refGene",all.x=TRUE)
#   #left_join(dataframe,omiminfo,by="Gene.refGene") %>%
#   #write.csv(.,file = "omiminfo.csv",row.names=FALSE)


complete <- dplyr::left_join(dataframe,omimdb,by="Gene.refGene")
write.csv(complete,"with_omim_IonCode0101.csv",row.names = FALSE)

