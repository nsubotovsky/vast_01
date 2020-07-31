rm(list=ls())
gc()


library(plyr)
library(dplyr)
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
    return( file.path( getwd(), "data", "template", "CGCS-Template-NodeTypes.csv" ) )
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

graph.process.financial.swap <- function( dt )
{
    dt <- dt %>%
            rowwise() %>%
            dplyr::mutate( Weight=ifelse( toString( Source ) %>% startsWith( 'pers_' ), Weight, -Weight ) )
    
    dt[ dt$Weight<0 ,c('Source', 'Target')] <- dt[ dt$Weight<0 ,c('Target', 'Source')]
    return( dt )
}

graph.process.translate.finance <- function( dt, demographicsDt )
{
    financialDt <- dt[ dt$eType == '5_financial', ]
    categories <- ( financialDt %>% select( Target ) %>% left_join( ( demographicsDt %>% dplyr::rename(Target=NodeID) ), by='Target' ) )$Category
    dt[ dt$eType == '5_financial', c('Target') ] <- categories
    return( dt )
}

refdata.demographics.load <- function( path )
{
    
    dt <- read_csv(path, 
                   col_types = cols(NodeID = col_integer())

    ) %>% as.data.table()
    return( dt )
}


## process all candidates

nodetypes <- refdata.get.path.full() %>%
    refdata.nodetypes.load() %>%
    refdata.process.types()


demographics <- refdata.get.path.demographics() %>%
    refdata.demographics.load() %>%
    rowwise() %>%
    dplyr::mutate( NodeID=paste0( "fin_", ( NodeID %>% toString() ) ) )


for (i in 1:5)
{
    path <- graph.get.path.candidate( i )
    sprintf('Working on %s', path) %>% print()
    
    graph <- graph.get.path.candidate( i ) %>%
        graph.load() %>%
        graph.process.edges() %>%
        graph.process.dates() %>%
        
        graph.process.prefixTypes( nodetypes ) %>%
        graph.process.financial.swap() %>%
        graph.process.translate.finance( demographics )
    
    
    newPath <- path %>% str_replace( '.csv', '.fix.csv' )
    sprintf('saving on %s', newPath) %>% print()
    graph %>% write_csv( newPath )
}



## do for template

nodetypes <- refdata.get.path.template() %>%
    refdata.nodetypes.load() %>%
    refdata.process.types()


path <- graph.get.path.template()
sprintf('Working on %s', path) %>% print()

graph <- graph.get.path.template() %>%
    graph.load() %>%
    graph.process.edges() %>%
    graph.process.dates() %>%
    
    graph.process.prefixTypes( nodetypes ) %>%
    graph.process.financial.swap() %>%
    graph.process.translate.finance( demographics )


newPath <- path %>% str_replace( '.csv', '.fix.csv' )
sprintf('saving on %s', newPath) %>% print()
graph %>% write_csv( newPath )

