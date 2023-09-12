library(jsonlite)
library(httr)
library(purrr)
library(lubridate)
library(tibble)
library(dplyr)
library(readr)

DW_API <- Sys.getenv("DW_API_KEY")

cdc_cdt_url <- "https://covid.cdc.gov/covid-data-tracker/COVIDData/getAjaxData?id=US_MAP_DATA"

get_cdc_covid_data_tracker <- function(url) {
  tryCatch(
    {
      raw_json_list <- read_json(cdc_cdt_url)
      
      return(raw_json_list)
    },
    error = function(e) {
      message(paste("An error occured with fetching the CDC Data:", 
                    e, "on", Sys.Date(), "\n"))
    }
  )
}

get_cdc_update_date <- function(json) {
  tryCatch(
    {
      date <- pluck(json, "CSVInfo") %>%
        pluck("update") %>% 
        parse_date_time(
          orders = "b d Y I:Mp",
          truncated = 3
        )
      return(date)
    },
    error = function(e) {
      message(paste("An error occured with parsing the CDC update date:", 
                    e, "on", Sys.Date(), "\n"))
    }
  )
}

get_last_cdc_update_date <- function(vec) {
  tryCatch(
    {
      date <- parse_date_time(
        vec,
        orders = "Y-m-dT",
        truncated = 3
      ) %>% 
        max()
      return(date)
    },
    error = function(e) {
      message(paste("An error occured with parsing the last CDC update date:", 
                    e, "on", Sys.Date(), "\n"))
    }
  )
}

republish_chart <- function(API_KEY, chartID, data, subtitle = NULL, 
                            title = NULL, colors = NULL, 
                            tooltip = NULL, legend = NULL, 
                            axes = NULL, notes) {
  
  # PUT request to refresh data as per: https://developer.datawrapper.de/reference/putchartsiddata
  dataRefresh <- PUT(url = paste0("https://api.datawrapper.de/v3/charts/", 
                                  chartID, "/data"),
                     add_headers(authorization = paste("Bearer", 
                                                       API_KEY, 
                                                       sep = " ")),
                     body = format_csv(data))
  
  call_back <- list(metadata = list())
  
  # This section adds chart title, subtitle, colors, tooltip, legend, and axes, if needed
  if (!is.null(title)) {
    call_back$title <- title
  }
  
  if (!is.null(subtitle)) {
    call_back$metadata$describe$intro <- subtitle   
  }
  
  if (!is.null(colors)) {
    call_back$metadata$visualize$`custom-colors` <- colors
  }
  
  if (!is.null(tooltip)) {
    call_back$metadata$visualize$tooltip <- tooltip
  }
  
  if (!is.null(legend)) {
    call_back$metadata$visualize$legend <- legend
  }
  
  if (!is.null(axes)) {
    call_back$metadata$axes <- axes
  }
  
  # Typically I always need to update the caption, but this can be 
  # moved to a conditional
  call_back$metadata$annotate$notes <- notes
  
  # PATCH request to update chart properties as per
  # https://developer.datawrapper.de/reference/patchchartsid
  notesRes <- PATCH(url = paste0("https://api.datawrapper.de/v3/charts/", 
                                 chartID),
                    add_headers(authorization = paste("Bearer", API_KEY, 
                                                      sep = " ")),
                    body = call_back,
                    encode = "json")
  
  # POST request to republish chart
  # https://developer.datawrapper.de/reference/postchartsidpublish
  publishRes <- POST(
    url = paste0("https://api.datawrapper.de/v3/charts/", 
                 chartID, "/publish"),
    add_headers(authorization = paste("Bearer", 
                                      API_KEY, 
                                      sep = " "))
  )
  
  list(dataRefresh, notesRes, publishRes) -> resList
  
  # Check for errors
  if (any(map_lgl(resList, http_error))) {
    which(map_lgl(resList, http_error))[1] -> errorIdx
    
    stop_for_status(resList[[errorIdx]], task = paste0("update step ",
                                                       errorIdx, 
                                                       " of chart ", 
                                                       chartID))
    
  } else {
    message(paste0("Chart ", chartID, " updated successfully"))
  }
  
}

cdc_cdt_df <- read_csv("./data/cdc_cdt.csv",
                       col_names = T,
                       col_types = cols(.default = col_character()))

cdc_cdt_data_raw <- get_cdc_covid_data_tracker(cdc_cdt_url)
cdc_update_date <- get_cdc_update_date(cdc_cdt_data_raw)
last_cdc_update_date <- get_last_cdc_update_date(cdc_cdt_df$data_update_dte)

if (cdc_update_date > last_cdc_update_date) {
  
  cdc_cdt_df_new <- cdc_cdt_data_raw %>% 
    pluck("US_MAP_DATA") %>% 
    map_dfr(`[`) %>% 
    mutate(data_update_dte = cdc_update_date,
           across(everything(), as.character))
  
  cdc_cdt_df_full <- bind_rows(cdc_cdt_df_new, cdc_cdt_df)
  
  write_csv(cdc_cdt_df_full, "./data/cdc_cdt.csv")
  
  map_states <- sort(c(state.abb, "DC"))
  
  hosp_maps_df <- cdc_cdt_df_new %>% 
    select(abbr, fips, name, collection_date, 
           total_adm_all_covid_confirmed_past_7days_per_100k,
           pct_chg_avg_adm_all_covid_confirmed_per_100k) %>% 
    filter(abbr %in% map_states)
  
  write_csv(hosp_maps_df, "./visualizations/cdc_cdt_hosp_maps.csv")
  
  republish_chart(DW_API, chartID = "NbGEg", data = hosp_maps_df,
                  notes = paste0("Per 100,000 residents. Data current as of ",
                                 format(
                                   max(
                                     base::as.Date(hosp_maps_df$collection_date)
                                     ), "%m/%d/%Y")))
  
  Sys.sleep(5)
  
  republish_chart(DW_API, chartID = "rcQEt", data = hosp_maps_df,
                  notes = paste0(
                    "From prior week. Hospitalizations per 100,000 residents.",
                    " Data current as of ",
                                 format(
                                   max(
                                     base::as.Date(hosp_maps_df$collection_date)
                                   ), "%m/%d/%Y")))
  
  
}
