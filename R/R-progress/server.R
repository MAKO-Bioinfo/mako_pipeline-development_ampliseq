# This is a visualization app for Mako's annotated vcf files
# this is the server.R file
# Jack Yen
# March 3rd, 2016

library(shiny)
library(ggplot2)
library(DT)
library(dplyr)
library(parallel)

#library(DMwR)
# Define server logic required to summarize and view the 
# selected dataset
shinyServer(function(input, output, session) {
  
  # Return the requested dataset
  dataset <- reactive({
    inFile <- input$file1
    if(!is.null(inFile$datapath))
    {
      dataset=read.csv(inFile$datapath, header=T, sep=',')
      v <- as.vector(t(dataset[1,]))
      dataset=dataset[-1,]
      
      for (i in 1:length(v)){
        if (v[i] == "N") {dataset[,i] <- as.numeric(dataset[,i])}
        else {dataset[,i] <- as.factor(dataset[,i])}
      }
      
    dataset
    #remove_column <- names(dataset()) %in% c ("AAChange.refGene","CLNACC","CLNDSDBID","genomicSuperDups","SIFT_pred","Polyphen2_HDIV_pred","Polyphen2_HVAR_pred","LRT_pred","MutationTaster_pred","MutationAssessor_pred","FATHMM_pred","RadialSVM_pred","LR_pred","CADD_phred")
    #dataset <- dataset[!remove_column]
    }
  })
  #remove_column <- names(dataset()) %in% c ("AAChange.refGene","CLNACC","CLNDSDBID","genomicSuperDups","SIFT_pred","Polyphen2_HDIV_pred","Polyphen2_HVAR_pred","LRT_pred","MutationTaster_pred","MutationAssessor_pred","FATHMM_pred","RadialSVM_pred","LR_pred","CADD_phred")
  #dataset <- dataset[!remove_column]
  
  # output$select_table <- renderDataTable({
  #   dataset()
  #})
  output$select_table <- DT::renderDataTable({
    expr = DT::datatable(head(dataset(),n=input$obs),
                         escape=FALSE,
                         selection='single',
                         filter = "top",  #filter must be at bottom if there is Scroller
                         rownames = FALSE,
                         options=list(pageLength = 10, autoWidth=TRUE,
                                      columnDefs = list(list(width = '10px', targets = c(1, 2))),
                                      order = list(list(4, 'desc'))))
  })
#})

## this is the part that does download feature 

# Generate a summary of the dataset
# output$summary <- renderPrint({
#   dataset <- datasetInput()
#   summary(dataset)
# })


output$downloadReport <- downloadHandler(
  filename = function(){
    paste('my-report',sep='.', switch(
      input$format, PDF = 'pdf', HTML = 'html', Word = 'docx'
      ))
},

   content = function(file){
     #s = input$file1
     src <- normalizePath('report.Rmd')
     # temporarily switch to the temp dir, in case you do not have write
     # permission to the current working directory
     owd <- setwd(tempdir())
     on.exit(setwd(owd))
     file.copy(src, 'report.Rmd')
     
     library(rmarkdown)
     out <- render('report.Rmd', switch(
       input$format,
       PDF = pdf_document(), HTML = html_document(), Word = word_document()
     ))
     file.rename(out, file)
   }
)

})
  # 
  # print('try to render')
  # out <- render(markdownFile, switch(
  #   input$format,
  #   PDF = pdf_document(), HTML = html_document(), Word = word_document()
  # ))
  # file.rename(out, file)
  # }
  #)


