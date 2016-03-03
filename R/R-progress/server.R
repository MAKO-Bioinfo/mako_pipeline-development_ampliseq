library(shiny)
library(ggplot2)
library(DT)
library(dplyr)

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
    dataset1 <- dataset
    }
  })
  
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
                                      columnDefs = list(list(width = '20px', targets = c(1, 2))),
                                      order = list(list(4, 'desc'))))
  })
})

## this is the part that does download feature 

# Generate a summary of the dataset
# output$summary <- renderPrint({
#   dataset <- datasetInput()
#   summary(dataset)
# })

# Show the first "n" observations
# output$table <- DT::renderDataTable(DT::dataTable({
#   data <- datasetInput
#   data
#   output$select_table <- renderDataTable(datasetInput)
# })

# output$downloadReport <- downloadHandler(
#   filename = function(){
#     paste('my-report',sep='.', switch(
#       input$format, PDF = pdf, HTML = 'html', Word = 'docx'
#       ))
#   },
#   content = function(file){
#     s = input$file1
#     
#   print('try to render')
#   out <- render(markdownFile, switch(
#     input$format,
#     PDF = pdf_document(), HTML = html_document(), Word = word_document()
#   ))
#   file.rename(out, file)
#   }
#   )


