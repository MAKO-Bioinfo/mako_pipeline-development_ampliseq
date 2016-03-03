library(shiny)

# Load the ggplot2 package which provides
# the 'mpg' dataset.
library(ggplot2)

## this part is edited 2/29/2016

mydata <- read.csv(file = '/Users/jack/CHP2/R_2016_01_15_13_52_13_user_S5-00391-3-Cancer_Hotspot_Panel_v2.vcf/TSVC_variants_IonCode_0101.myanno.hg19_multianno.csv',header=TRUE)

# Define a server for the Shiny app
shinyServer(function(input, output) {
  
  # Filter data based on selections
  output$table <- DT::renderDataTable(DT::datatable({
    #data <- mpg
    data <- mydata
    if (input$man != "All") {
      data <- data[data$manufacturer == input$man,]
    }
    if (input$cyl != "All") {
      data <- data[data$cyl == input$cyl,]
    }
    if (input$trans != "All") {
      data <- data[data$trans == input$trans,]
    }
    data
  }))
  
})