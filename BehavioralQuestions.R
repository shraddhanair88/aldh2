##Filter the 4 Drinking Questions
library(tidyverse)
library(bigrquery)
library(dplyr)

# This query represents dataset "hasgeneticdatascratch" for domain "survey" and was generated for All of Us Controlled Tier Dataset v8
dataset_23830138_survey_sql <- paste("
    SELECT
        answer.person_id,
        answer.question,
        answer.answer,
        answer.survey_version_name  
    FROM
        `ds_survey` answer   
    WHERE
        (
            question_concept_id IN (1586198, 1586201, 1586207, 1586213)
        )", sep="")

# Formulate a Cloud Storage destination path for the data exported from BigQuery.
# NOTE: By default data exported multiple times on the same day will overwrite older copies.
#       But data exported on a different days will write to a new location so that historical
#       copies can be kept as the dataset definition is changed.
survey_23830138_path <- file.path(
  Sys.getenv("WORKSPACE_BUCKET"),
  "bq_exports",
  Sys.getenv("OWNER_EMAIL"),
  strftime(lubridate::now(), "%Y%m%d"),  # Comment out this line if you want the export to always overwrite.
  "survey_23830138",
  "survey_23830138_*.csv")
message(str_glue('The data will be written to {survey_23830138_path}. Use this path when reading ',
                 'the data into your notebooks in the future.'))

# Perform the query and export the dataset to Cloud Storage as CSV files.
# NOTE: You only need to run `bq_table_save` once. After that, you can
#       just read data from the CSVs in Cloud Storage.
bq_table_save(
  bq_dataset_query(Sys.getenv("WORKSPACE_CDR"), dataset_23830138_survey_sql, billing = Sys.getenv("GOOGLE_PROJECT")),
  survey_23830138_path,
  destination_format = "CSV")


# Read the data directly from Cloud Storage into memory.
# NOTE: Alternatively you can `gsutil -m cp {survey_23830138_path}` to copy these files
#       to the Jupyter disk.
read_bq_export_from_workspace_bucket <- function(export_path) {
  col_types <- cols(survey = col_character(), question = col_character(), answer = col_character(), survey_version_name = col_character())
  bind_rows(
    map(system2('gsutil', args = c('ls', export_path), stdout = TRUE, stderr = TRUE),
        function(csv) {
          message(str_glue('Loading {csv}.'))
          chunk <- read_csv(pipe(str_glue('gsutil cat {csv}')), col_types = col_types, show_col_types = FALSE)
          if (is.null(col_types)) {
            col_types <- spec(chunk)
          }
          chunk
        }))
}
dataset_23830138_survey_df <- read_bq_export_from_workspace_bucket(survey_23830138_path)

dim(dataset_23830138_survey_df)

head(dataset_23830138_survey_df, 5)

alldrinkingquestions <- dataset_23830138_survey_df
unique(alldrinkingquestions$question)

alcohol_participant <- alldrinkingquestions %>%
  filter(question == "Alcohol: Alcohol Participant")

drink_frequency_past_year <- alldrinkingquestions %>%
  filter(question == "Alcohol: Drink Frequency Past Year")

average_daily_drink_count <- alldrinkingquestions %>%
  filter(question == "Alcohol: Average Daily Drink Count")

six_or_more_drinks_occurrence <- alldrinkingquestions %>%
  filter(question == "Alcohol: 6 or More Drinks Occurrence")

#now cross each with the 4 rs671/nors671 cohorts

# Using only the four main tables
# 1. rs671_people (all rs671 participants)
# 2. no_rs671_people (all non-rs671 participants)
# 3. rs671_people_asian (rs671 participants who are Asian)
# 4. no_rs671_people_asian (non-rs671 participants who are Asian)

# Make person_id character in the drinking tables
alcohol_participant <- alcohol_participant %>% mutate(person_id = as.character(person_id))
drink_frequency_past_year <- drink_frequency_past_year %>% mutate(person_id = as.character(person_id))
average_daily_drink_count <- average_daily_drink_count %>% mutate(person_id = as.character(person_id))
six_or_more_drinks_occurrence <- six_or_more_drinks_occurrence %>% mutate(person_id = as.character(person_id))

# Join with the four main cohorts, semijoin so we can filter down
alcohol_participant_rs671 <- semi_join(alcohol_participant, rs671_people, by = "person_id")
alcohol_participant_nors671 <- semi_join(alcohol_participant, no_rs671_people, by = "person_id")
alcohol_participant_rs671_asian <- semi_join(alcohol_participant, rs671_people_asian, by = "person_id")
alcohol_participant_nors671_asian <- semi_join(alcohol_participant, no_rs671_people_asian, by = "person_id")

drink_freq_rs671 <- semi_join(drink_frequency_past_year, rs671_people, by = "person_id")
drink_freq_nors671 <- semi_join(drink_frequency_past_year, no_rs671_people, by = "person_id")
drink_freq_rs671_asian <- semi_join(drink_frequency_past_year, rs671_people_asian, by = "person_id")
drink_freq_nors671_asian <- semi_join(drink_frequency_past_year, no_rs671_people_asian, by = "person_id")

avg_drink_rs671 <- semi_join(average_daily_drink_count, rs671_people, by = "person_id")
avg_drink_nors671 <- semi_join(average_daily_drink_count, no_rs671_people, by = "person_id")
avg_drink_rs671_asian <- semi_join(average_daily_drink_count, rs671_people_asian, by = "person_id")
avg_drink_nors671_asian <- semi_join(average_daily_drink_count, no_rs671_people_asian, by = "person_id")

sixplus_rs671 <- semi_join(six_or_more_drinks_occurrence, rs671_people, by = "person_id")
sixplus_nors671 <- semi_join(six_or_more_drinks_occurrence, no_rs671_people, by = "person_id")
sixplus_rs671_asian <- semi_join(six_or_more_drinks_occurrence, rs671_people_asian, by = "person_id")
sixplus_nors671_asian <- semi_join(six_or_more_drinks_occurrence, no_rs671_people_asian, by = "person_id")

# Function to produce counts + percentages excluding PMI responses
make_table_no_PMI <- function(df, var) {
  df_filtered <- df %>% filter(!answer %in% c("PMI: Prefer Not To Answer", "PMI: Skip"))
  
  df_filtered %>%
    count({{var}}, name = "count") %>%
    mutate(percent = 100 * count / sum(count)) %>%
    bind_rows(summarise(., !!deparse(substitute(var)) := "Total", count = sum(count), percent = sum(percent))) %>%
    mutate(percent = sprintf("%.2f", percent))
}

# Define desired order for Drink Frequency
drink_frequency_order <- c(
  "Drink Frequency Past Year: Never",
  "Drink Frequency Past Year: Monthly Or Less",
  "Drink Frequency Past Year: 2 to 4 Per Month",
  "Drink Frequency Past Year: 2 to 3 Per Week",
  "Drink Frequency Past Year: 4 or More Per Week"
)

# Define desired order for Average Daily Drink Count
avg_drink_order <- c(
  "Average Daily Drink Count: 1 or 2",
  "Average Daily Drink Count: 3 or 4",
  "Average Daily Drink Count: 5 or 6",
  "Average Daily Drink Count: 7 to 9",
  "Average Daily Drink Count: 10 or More"
)

# Function to make table with specified order
make_ordered_table <- function(df, var, order_vec) {
  df_filtered <- df %>% filter(!answer %in% c("PMI: Prefer Not To Answer", "PMI: Skip"))
  
  df_table <- df_filtered %>%
    count({{var}}, name = "count") %>%
    mutate(percent = 100 * count / sum(count)) %>%
    mutate(answer = factor(answer, levels = order_vec)) %>%
    arrange(answer) %>%
    bind_rows(summarise(., answer = "Total", count = sum(count), percent = sum(percent))) %>%
    mutate(percent = sprintf("%.2f", percent))
  
  return(df_table)
}

# Apply to Drink Frequency tables
drink_freq_rs671_table <- make_ordered_table(drink_freq_rs671, answer, drink_frequency_order)
drink_freq_nors671_table <- make_ordered_table(drink_freq_nors671, answer, drink_frequency_order)
drink_freq_rs671_asian_table <- make_ordered_table(drink_freq_rs671_asian, answer, drink_frequency_order)
drink_freq_nors671_asian_table <- make_ordered_table(drink_freq_nors671_asian, answer, drink_frequency_order)

# Apply to Average Daily Drink Count tables
avg_drink_rs671_table <- make_ordered_table(avg_drink_rs671, answer, avg_drink_order)
avg_drink_nors671_table <- make_ordered_table(avg_drink_nors671, answer, avg_drink_order)
avg_drink_rs671_asian_table <- make_ordered_table(avg_drink_rs671_asian, answer, avg_drink_order)
avg_drink_nors671_asian_table <- make_ordered_table(avg_drink_nors671_asian, answer, avg_drink_order)


# Make person_id the same type
rs671_people <- rs671_people %>% mutate(person_id = as.character(person_id))
alcohol_participant <- alcohol_participant %>% mutate(person_id = as.character(person_id))

# Filter alcohol_participant to only those in rs671_people
alcohol_participant_rs671 <- semi_join(alcohol_participant, rs671_people, by = "person_id")

# Check number of rows
table(alcohol_participant_rs671$answer)
nrow(alcohol_participant_rs671)