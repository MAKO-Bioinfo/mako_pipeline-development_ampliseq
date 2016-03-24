setwd("/Users/jack/CHP2/R_2016_01_15_13_52_13_user_S5-00391-3-Cancer_Hotspot_Panel_v2.vcf")
dataframe <- read.csv("TSVC_variants_IonCode_0101_03042016.myanno.hg19_multianno.csv",header = T)
omimdb <- read.csv("omiminfo.csv",header = T)
#omiminfo <- subset(omimdb,select=c('Gene.refGene','GeneName'))

<<<<<<< HEAD
omimdb2 <- read.csv("morbidmap.csv",header = T)
omiminfo2 <- subset(omimdb2,select=c('OMIM_Phenotype','Gene.refGene'))


library(tidyr)
library(dplyr)
omiminfo2 %>%
  mutate(Gene.refGene = strsplit(as.character(Gene.refGene),",")) %>%
  unnest(Gene.refGene) %>%
  write.csv(.,file = "omiminfo2.csv",row.names=FALSE)
=======

library(tidyr)
library(dplyr)
# omiminfo %>%
#   mutate(Gene.refGene = strsplit(as.character(Gene.refGene),",")) %>%
#   unnest(Gene.refGene) %>%
>>>>>>> e18b9dac3beb457e669e9223d433c6589caadf21
#   #merge(dataframe,omiminfo,by="Gene.refGene",all.x=TRUE)
#   #left_join(dataframe,omiminfo,by="Gene.refGene") %>%
#   #write.csv(.,file = "omiminfo.csv",row.names=FALSE)

<<<<<<< HEAD
omim_new_info <- read.csv("omiminfo2.csv",header = T) %>%
  # mutate_each_(funs(factor),omim_new_info$refGene) %>%
  # mutate_each_(funs(character),omim_new_info$refGene)
  group_by(refGene) %>%
  summarise_each(funs(paste(sort(.),collapse=","))) %>%
  write.csv(.,file="omim_new_info.csv",row.names=FALSE)
  #summarise_each(funs(sum))


omim_new_info <- read.csv('omim_new_info.csv',header = T)


complete <- dplyr::left_join(dataframe,omim_new_info,by="Gene.refGene") %>%
  write.csv(complete,"with_omim_IonCode0101_new.csv",row.names = FALSE)
=======

complete <- dplyr::left_join(dataframe,omimdb,by="Gene.refGene")
write.csv(complete,"with_omim_IonCode0101.csv",row.names = FALSE)
>>>>>>> e18b9dac3beb457e669e9223d433c6589caadf21

