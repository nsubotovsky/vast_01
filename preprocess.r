rm(list=ls())
gc()



library(dplyr)
library(plyr)
library(readr)
library(data.table)
library(lubridate)
library(tidyverse)
library(purrr)
library(glue)




############ graph paths ##############
#######################################


graph.get.path.candidate <- function( n )
{
    return( file.path( getwd(), "data", "Q1-Graph%s.csv" %>% sprintf( n ) ) )
}

graph.get.path.template <- function()
{
    return( file.path( getwd(), "data", "template", "CGCS-Template.csv" ) )
}

graph.get.path.full <- function()
{
    return( file.path( getwd(), "data", "full graph", "CGCS-GraphData.csv" ) )
}


########### refdata paths #############
#######################################


refdata.get.path.template <- function()
{
    return( file.path( getwd(), "data", "template", "CGCS-Template.csv" ) )
}

refdata.get.path.full <- function()
{
    return( file.path( getwd(), "data", "full graph", "CGCS-GraphData-NodeTypes.csv" ) )
}

refdata.get.path.demographics <- function()
{
    return( file.path( getwd(), "data", "refdata", "DemographicCategories.csv" ) )
}


############ Graph direct processing #############
##################################################

graph.load <- function( path )
{
    graph <- read_csv(path, 
                      col_types = cols(Source = col_integer(), 
                                       Target = col_integer(),
                                       Time = col_integer(), 
                                       eType = col_integer())
                      ) %>% as.data.table()
}


graph.process.dates <- function( dt )
{
    return( dt %>% mutate( Time=as.Date(as_datetime(Time + as.numeric(as.POSIXct("2025-01-01 00:00:00 GMT"))) )) );
}

graph.process.edges <- function( dt )
{
    return( 
        dt %>%
        mutate(
            eType=mapvalues(eType,
                            from = c(0,1,2,3,4,5,6),
                            to = c("0_emails", "1_phone", "2_sell", "3_buy", "4_author", "5_financial", "6_travels")
            ) %>%
        as.factor() )
    );
    
}


graph.process.prefixTypes <- function( dt, refdata )
{
    enrich_id <- function( idNum, idType )
    {
        strId <- toString(idNum)
        if ( idType == "1_person" ) return( paste0( "pers_", strId ) )
        if ( idType == "2_product" ) return( paste0( "prod_", strId ) )
        if ( idType == "3_document" ) return( paste0( "doc_", strId ) )
        if ( idType == "4_financial" ) return( paste0( "fin_", strId ) )
        if ( idType == "5_country" ) return( paste0( "count_", strId ) )
        return( paste0( "unkn_", strId ))
    }
    
    
    dt <- dt %>% left_join( ( refdata %>% dplyr::rename(Source=NodeID) %>% dplyr::rename(SourceType=NodeType) ), by='Source' )
    dt <- dt %>% mutate( Source=map2_chr(Source, SourceType, enrich_id) ) %>% select(-SourceType )
    
    
    dt <- dt %>% left_join( ( refdata %>% dplyr::rename(Target=NodeID) %>% dplyr::rename(TargetType=NodeType) ), by='Target' )
    dt <- dt %>% mutate( Target=map2_chr(Target, TargetType, enrich_id) ) %>% select(-TargetType )
    
    return( dt );
}



refdata.process.types <- function( dt )
{
    return( dt %>%
            mutate(
                NodeType=mapvalues(NodeType,
                                   from = c(1,2,3,4,5),
                                   to = c("1_person", "2_product", "3_document", "4_financial", "5_country")
           ) %>%
        as.factor() )
    );
}



refdata.nodetypes.load <- function( path )
{
    graph <- read_csv(path, 
                      col_types = cols(NodeID = col_integer(), 
                                       NodeType = col_integer())
    ) %>% as.data.table()
}








nodetypes <- refdata.get.path.full() %>%
    refdata.nodetypes.load() %>%
    refdata.process.types()




g2 <- graph.get.path.candidate( 1 ) %>%
    graph.load() %>%
    graph.process.edges() %>%
    graph.process.dates() %>%

    graph.process.prefixTypes( nodetypes )





##########################################



aaa<- graph[complete.cases(graph),]


a <- graph %>% filter(  eType == "6_travels" ) %>% select( Target, TargetLocation, TargetLatitude, TargetLongitude)
b <- graph %>% filter(  eType == "6_travels" ) %>% select( Source, SourceLocation, SourceLatitude, SourceLongitude)

# sorting stuff...
# select(Source, everything())


####################

