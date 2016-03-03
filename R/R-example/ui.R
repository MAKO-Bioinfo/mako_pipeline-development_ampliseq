library(shiny)

# Load the ggplot2 package which provides
# the 'mpg' dataset.
library(ggplot2)

# Define the overall UI
shinyUI(
  
  ###  
  fluidPage(
    titlePanel("Variant Annotation Report"),
    
    # Create a new Row in the UI for selectInputs
    fluidRow(
      column(4,
             selectInput("man",
                         "Gene Function:",
                         c("All",
                           unique(as.character(mydata$Func.refGene))))
      ),
      column(4,
             selectInput("trans",
                         "Gene Reference ID:",
                         c("All",
                           unique(as.character(mydata$Gene.refGene))))
      ),
      column(4,
             selectInput("cyl",
                         "SIFT Score:",
                         c("All",
                           unique(as.character(mydata$SIFT_score))))
      )
    ),
    # Create a new row for the table.
    fluidRow(
      DT::dataTableOutput("table")
      
    )
  )
)