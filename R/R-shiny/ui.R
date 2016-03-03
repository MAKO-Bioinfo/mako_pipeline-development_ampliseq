library(shiny)

# Load the ggplot2 package which provides
# the 'mpg' dataset.
library(ggplot2)

# Define the overall UI
shinyUI(navbarPage(title = div(img(src="MAKO_Genomics_small.jpg",height="40px"),
                               "Variant Annotation Report", align = "center"),
                   windowTitle = "Mako AmpliSeq Analysis",
                   tabPanel(
                     "Run Report",
                     fluidPage(
                       title = 'Mako AmpliSeq Runs',
                       sidebarLayout(sidebarPanel(width = 5,
                                                  #DT::dataTableOutput('select_table'),
                                                  #hr(),
                                                  #uiOutput('select_project_id'),
                                                  actionButton("renderReport", label = "Render Report"),
                                                  hr(),
                                                  radioButtons('format', 'Document format', c('PDF', 'HTML', 'Word'),
                                                               inline = TRUE),
                                                  downloadButton('downloadReport')
                       ),
                                 fluidRow(
                                   #div(uiOutput("run_report"))
                                   DT::datatable('table')
                                 )
                       )
                       )
                     )
                   )
)

#        
#      ))
#   )
# )
#fluidPage(
#  titlePanel("Variant Annotation Report"),

# # Create a new Row in the UI for selectInputs
# fluidRow(
#   column(4,
#          selectInput("man",
#                      "Gene Function:",
#                      c("All",
#                        unique(as.character(mydata$Func.refGene))))
#   ),
#   column(4,
#          selectInput("trans",
#                      "Gene Reference ID:",
#                      c("All",
#                        unique(as.character(mydata$Gene.refGene))))
#   ),
#   column(4,
#          selectInput("cyl",
#                      "SIFT Score:",
#                      c("All",
#                        unique(as.character(mydata$SIFT_score))))
#   )
# ),
# # Create a new row for the table.
# fluidRow(
#   DT::dataTableOutput("table")
#)
#)
#)