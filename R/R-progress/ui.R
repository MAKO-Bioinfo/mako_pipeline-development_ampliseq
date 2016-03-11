
# This is the user-interface definition of a Shiny web application.
# You can find out more about building applications with Shiny here:
#
# http://shiny.rstudio.com
#
# This is a visualization app for Mako's annotated vcf files
# Jack Yen
# March 3rd, 2016

library(shiny)

shinyUI(fluidPage(
  titlePanel(title="AmpliSeq Cancer Hotspot V2 Annotation Results"),
  sidebarLayout(
    sidebarPanel(
      fileInput('file1', 'Please Upload the annotated csv file',
                accept=c('text/csv', 'text/comma-separated-values,text/plain', '.csv'),multiple = TRUE),
      
      numericInput("obs", "Number of observations to view:", 10),
      
      helpText("Note: while the data view will show only the specified",
               "number of observations, the summary will still be based",
               "on the full dataset."),
      #actionButton("getcsv","View"),
      # br(),
      # br(),
      # submitButton("Update View"),
      #br(),
      radioButtons('format', 'Document format', c('PDF', 'HTML', 'Word'),
                   inline = TRUE),
      downloadButton('downloadReport'),
      br(),
      br(),
      img(src="MAKO_Genomics_small.jpg", height = 200, width = 200),
      br(),
      h5("Mako Bioinformatics")
      #p("Jack Yen")
      # p("View source on",
      #   a("github", 
      #     href = "https://github.com/ss6012/dataprod_shinyapp", target = "_blank")),
      # p("Visit", 
      #   a("this page", href = "https://ss6012.github.io/Slidify-Doc-Shiny-App/", target = "_blank"), "for documentation"),
      # a("Sample.csv", href = "https://db.tt/NjtUG4GG", target = "_blank")
      
    ),
    
    ### output display panel
    
    mainPanel(h4("Summary of the annotated vcf file from ANNOVAR"),
              p("For more information about ANNOVAR, please visit the ",
                a("ANNOVAR homepage.", 
                  href = "http://annovar.openbioinformatics.org/en/latest/user-guide/region/")),
               DT::dataTableOutput("select_table")
      #width = 6,
      #fluidRow(
      #  div(uiOutput("select_table"))

      )
      
    )
    
  )
)

