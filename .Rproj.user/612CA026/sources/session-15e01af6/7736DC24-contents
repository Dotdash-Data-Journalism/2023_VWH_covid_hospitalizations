library(dplyr)
library(tidyr)
library(readr)
library(httr)
library(lubridate)
library(tibble)
library(purrr)
library(stringr)

DW_API <- Sys.getenv("DW_API_KEY")
SCT_PW <- Sys.getenv("SCT_PW")
SCT_USER <- Sys.getenv("SCT_USER")

### FUNCTIONS ###
## Function to retrieve CDC variant data ##
cdc_fetch <- function(resource, format, limit, user, pw) {
  tryCatch(
    {
      res <- GET(url = paste0("https://data.cdc.gov/resource/", resource, ".", 
                              format, "?$limit=", limit),
                 authenticate(user = user, password = pw))
      
      stop_for_status(res)
      
      Sys.sleep(10)
      
      return(res)
      
    },
    error = function(e) {
      message(paste("An error occured with fetching the CDC Data:", 
                    e, "on", Sys.Date(), "\n"))
    },
    warning = function(w) {
      message(paste("A warning occured with fetching the CDC Data:", w))
      return(NA)
    }
  )
}

## Function to republish chart via Datawrapper API ##
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

### ANALYSIS ### 
# VW Health color scheme.
VWH_color_scheme <- c("#28c4d8", "#0a383f", "#3fba6b", "#9659b2", "#fecb00", 
                      "#f24a00", "#ec4c64", "#888888", "#222222")

# Getting latest variant data. Limit can be altered.
cdc_fetch(resource = "jr58-6ysp", format = "csv", 
          limit = "5000000", user = SCT_USER, pw = SCT_PW) -> variant_res

Sys.sleep(3)

content(variant_res, 
        as = "parsed",
        type = "text/csv",
        encoding = "UTF-8",
        col_names = T,
        cols(.default = col_character())) %>% 
  mutate(week_ending = as_date(week_ending),
         creation_date = as_date(creation_date),
         share = as.numeric(share) * 100) -> variant_data

### Making data for Datawrapper charts ###
## Area chart nationally for last ~3 months of variant proportions by week ##
variant_data %>% 
    filter(week_ending >= (max(week_ending) %m-% months(3)), 
           usa_or_hhsregion == "USA") %>% 
    group_by(week_ending, variant) %>% 
    filter(creation_date == max(creation_date)) %>% 
    group_by(week_ending) %>% 
    slice_max(n = 3, order_by = share) %>% 
    ungroup() %>% 
    select(week_ending, variant, share) %>% 
    pivot_wider(names_from = variant, values_from = share) %>% 
    mutate(across(where(is.numeric), ~replace_na(.x, 0))) %>% 
    rowwise() %>% 
    mutate(Other = 100 - sum(c_across(-week_ending), na.rm = T)) %>% 
    ungroup() -> variant_area_chart

variant_area_chart %>% 
  filter(week_ending == max(week_ending)) %>% 
  pivot_longer(!week_ending, names_to = "variant", values_to = "prop") %>% 
  arrange(desc(prop)) %>% 
  mutate(color = VWH_color_scheme[1:nrow(.)]) %>% 
  select(variant, color) %>% 
  deframe() %>% 
  as.list() -> area_dw_colors

republish_chart(API_KEY = DW_API,
               chartID = "U9shr",
               data = variant_area_chart,
               colors = area_dw_colors,
               notes = paste0("Three most recent weeks are from CDC Nowcast model. ",
                              "Variants with low estimated proportions of total ",
                              "caseload included in \"Other\" category. ",
                              "Data is current as of ",
                              format(Sys.Date(), "%m/%d/%Y"), "."))

# Writing out CSV for reference
write_csv(variant_area_chart, "./visualizations/variant_area_chart.csv")

## Maps for current week of variant proportions by HHS region ##
# HHS regions
hhsRegions <- tibble(
  ID = map_chr(1:10, ~paste("Region", as.character(.x), sep = " ")),
  States = c(paste0("Connecticut, Maine, Massachusetts, New Hampshire, ",
                    "Rhode Island, and Vermont"),
             "New Jersey, New York, Puerto Rico, and the Virgin Islands",
             paste0("Delaware, District of Columbia, Maryland, Pennsylvania, ",
             "Virginia, and West Virginia"),
             paste0("Alabama, Florida, Georgia, Kentucky, Mississippi, ",
             "North Carolina, South Carolina, and Tennessee"),
             "Illinois, Indiana, Michigan, Minnesota, Ohio, and Wisconsin",
             "Arkansas, Louisiana, New Mexico, Oklahoma, and Texas",
             "Iowa, Kansas, Missouri, and Nebraska",
             "Colorado, Montana, North Dakota, South Dakota, Utah, and Wyoming",
             paste0("Arizona, California, Hawaii, Nevada, American Samoa, ",
             "Commonwealth of the Northern Mariana Islands, ",
             "Federated States of Micronesia, Guam, Marshall Islands, ",
             "and Republic of Palau"),
             "Alaska, Idaho, Oregon, and Washington"
  )
)

variant_data %>% 
  group_by(usa_or_hhsregion) %>% 
  summarize(max_date = max(week_ending)) %>% 
  pull(max_date) %>% 
  min(., na.rm = T) -> most_recent_week

variant_data %>% 
  filter(week_ending == most_recent_week) -> variant_data_latest

variant_data_latest %>% 
  filter(usa_or_hhsregion == "USA", creation_date == min(creation_date)) %>% 
  slice_max(n = 3, order_by = share) %>% 
  pull(variant) -> map_viz_variants

chart_ids <- c("Ha3XE", "tbbMo", "3qNe5")
  
variant_data_latest %>%   
  filter(week_ending == most_recent_week, creation_date == min(creation_date),
         usa_or_hhsregion != "USA", variant %in% map_viz_variants) %>% 
  select(usa_or_hhsregion, week_ending, variant, share) %>% 
  rename(ID = usa_or_hhsregion, day_of_week_end = week_ending) %>% 
  pivot_wider(names_from = variant, values_from = share) %>% 
  mutate(ID = paste("Region", ID)) %>% 
  left_join(hhsRegions, by = "ID") -> full_data_variant_map

Sys.sleep(3)

# Looping through the three maps to update data and potentially variants
walk2(map_viz_variants, chart_ids, function(x, y) {
  Sys.sleep(5)
  full_data_variant_map %>% 
    select(ID, day_of_week_end, !!x, States) -> df
  
  paste0(
    "Click on the buttons below to see the percentage of the most prevalent COVID-19 ",
    "strains in the US:\n<br>\n\n<span style=line-height:30px>\n\n<a target=\"_self\" ",
    "href=\"https://datawrapper.dwcdn.net/Ha3XE/\" style=\"background:#28c4d8; padding:1px 6px;",
    " border-radius:5px; color:#ffffff; font-weight:400; box-shadow:0px 0px 7px 2px rgba(0,0,0,0.07);",
    " cursor:pointer;\"> ",
    map_viz_variants[1],
    " </a> &nbsp;\n\n<a target=\"_self\" href=\"https://datawrapper.dwcdn.net/tbbMo/\" ",
    "style=\"background:#9659b2; padding:1px 6px; border-radius:5px; color:#ffffff;",
    "font-weight:400; box-shadow:0px 0px 7px 2px rgba(0,0,0,0.07); cursor:pointer;\"> ",
    map_viz_variants[2],
    " </a> &nbsp;\n\n<a target=\"_self\" href=\"https://datawrapper.dwcdn.net/3qNe5/\" ",
    "style=\"background:#3fba6b; padding:1px 6px; border-radius:5px; color:#ffffff;",
    " font-weight:400; box-shadow:0px 0px 7px 2px rgba(0,0,0,0.07); cursor:pointer;\">",
    map_viz_variants[3],
    " </a> &nbsp;"
  ) -> description
  
  axes_list <- list(keys = "ID", values = x)
  tooltip_list <- list(body = paste0("Variant ",
                                     x, 
                                     " proportion: <strong>{{ ROUND(",
                                     str_to_lower(str_remove_all(x, "\\.")),
                                     ", 2) }} %</strong>\n<br><hr>\nRegion includes states/territories: <strong>{{ states }}</strong>"))
  
  legend_list <- list(title = paste0(
    "Proportion of COVID-19 cases attributed to ", x, " variant"
  ))
  
  write_csv(df, paste0("./visualizations/", y, "_map.csv"))
  
  republish_chart(API_KEY = DW_API,
                 chartID = y,
                 data = df,
                 subtitle = description,
                 tooltip = tooltip_list,
                 legend = legend_list,
                 axes = axes_list,
                 notes = paste0(
                   "COVID-19 variant data via CDC Nowcast model for week ending in ",
                   format(unique(df$day_of_week_end), "%m/%d/%Y"),
                   " and updated as of ",
                   format(Sys.Date(), "%m/%d/%Y"),
                   "."
                 ))
  
})

