rm(list=ls())
gc()



library(dplyr)
library(plyr)
library(readr)
library(data.table)
library(lubridate)
#library(hash)
library(tidyverse)
library(purrr)


graph_path <- "D:/Nicolas/Estudios/Maestria/3. [Sab] Visualizacion/TP Vast MC1/data/Q1-Graph1.csv"

nodetype_ref <- "D:/Nicolas/Estudios/Maestria/3. [Sab] Visualizacion/TP Vast MC1/data/full graph/CGCS-GraphData-NodeTypes.csv"




factor_edgetypes <- function( col_edgeTypes )
{
    return( col_edgeTypes %>%
                mapvalues( from = c(0,1,2,3,4,5,6), to = c("0_emails", "1_phone", "2_sell", "3_buy", "4_author", "5_financial", "6_travels")) %>%
                as.factor() )
}


factor_nodetypes <- function( col_nodeTypes )
{
    return( col_nodeTypes %>%
                mapvalues( from = c(1,2,3,4,5), to = c("1_person", "2_product", "3_document", "4_financial", "5_country")) %>%
                as.factor() )
}





graph <- read_csv(graph_path, 
                  col_types = cols(Source = col_integer(), 
                                   Target = col_integer(), Time = col_integer(), 
                                   eType = col_integer())) %>% as.data.table()


# Fix time with offset
base_date <- as.numeric(as.POSIXct("2025-01-01 2:13:46 GMT"))
graph <- graph %>% mutate( Time=as.Date(as_datetime(Time + base_date) ))


# convert eType to factor
graph$eType <- graph$eType %>% factor_edgetypes()


nodetypes <- read_csv(nodetype_ref, 
                      col_types = cols(NodeID = col_integer(), 
                                       NodeType = col_integer())) %>% as.data.table()

nodetypes$NodeType <- nodetypes$NodeType %>% factor_nodetypes()


graph <- left_join( graph, ( nodetypes %>% dplyr::rename(Source=NodeID) %>% dplyr::rename(SourceType=NodeType) ), by='Source' )

graph <- left_join( graph, ( nodetypes %>% dplyr::rename(Target=NodeID) %>% dplyr::rename(TargetType=NodeType) ), by='Target' )


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


graph <- graph %>% mutate( Source=map2_chr(Source, SourceType, enrich_id) ) %>% select(-SourceType )


graph <- graph %>% mutate( Target=map2_chr(Target, TargetType, enrich_id) ) %>% select(-TargetType )


aaa<- graph[complete.cases(graph),]


a <- graph %>% filter(  eType == "6_travels" ) %>% select( Target, TargetLocation, TargetLatitude, TargetLongitude)
b <- graph %>% filter(  eType == "6_travels" ) %>% select( Source, SourceLocation, SourceLatitude, SourceLongitude)

# sorting stuff...
# select(Source, everything())


####################


h <- hash(keys=nodetypes$NodeID, values=nodetypes$NodeType)



graph %>% mutate( aSource=h[[toString(Source)]] ) %>% head()


map( graph[['Source']], function(id) { return( h[[toString(id)]] ); } )



toNamed <- function( nodeId, nodeTable )
{
    
}













graph <- graph %>% filter( eType==6  )




View(graph)



