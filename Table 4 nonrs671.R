# Table 4 nonrs671.R
# Title: HYBRID EFFICIENT Workflow for Non-rs671 Cohort
#
# Description:
# This script uses a hybrid approach to analyze the alcohol consumption patterns
# for participants who DO NOT have the rs671 variant. It exports only the
# necessary data from BigQuery to CSVs and then loads them into R for analysis.

#-------------------------------------------------------------------------------
# SECTION 1: EFFICIENT DATA EXPORT & LOADING FOR NON-RS671 COHORT


# --- 1a. Setup: Load all necessary libraries ---
# Make sure you have these packages installed:
# install.packages(c("tidyverse", "bigrquery", "janitor"))

library(tidyverse)
library(bigrquery)
library(janitor)

# --- 1b. Define Target Questions ---
# Define the concept IDs for all alcohol questions you want to include.
target_question_concept_ids <- c(
  1586198, # In your entire life, have you had at least 1 drink...
  1586201, # How often did you have a drink containing alcohol in the past year?
  1586207, # On a typical day when you drink, how many drinks do you have?
  1586213  # How often did you have six or more drinks on one occasion...
)

# --- 1c. Load Filtered Survey Data for Non-rs671 Cohort ---
# This query exports the target alcohol questions for participants WITHOUT the rs671 variant.
dataset_non_rs671_survey_sql <- paste(
  "SELECT person_id, question_concept_id, question, answer FROM `ds_survey` WHERE question_concept_id IN (",
  paste(target_question_concept_ids, collapse = ", "),
  ") AND person_id NOT IN (SELECT person_id FROM `cb_variant_to_person` CROSS JOIN UNNEST(person_ids) AS person_id WHERE vid = '12-111803962-G-A')"
)

survey_non_rs671_path <- file.path(
  Sys.getenv("WORKSPACE_BUCKET"),
  "bq_exports",
  Sys.getenv("OWNER_EMAIL"),
  "filtered_survey_alcohol_non_rs671",
  "filtered_survey_alcohol_non_rs671_*.csv")

message(str_glue('Exporting filtered survey data for non-rs671 cohort to {survey_non_rs671_path}.'))

bq_table_save(
  bq_dataset_query(Sys.getenv("WORKSPACE_CDR"), dataset_non_rs671_survey_sql, billing = Sys.getenv("GOOGLE_PROJECT")),
  survey_non_rs671_path,
  destination_format = "CSV")

# --- 1d. Load Person Data for Non-rs671 Cohort ---
# This query exports demographic data for participants WITHOUT the rs671 variant.
dataset_non_rs671_person_sql <- "
    SELECT person_id, race_concept_id, ethnicity_concept_id
    FROM `person`
    WHERE person_id NOT IN (SELECT person_id FROM `cb_variant_to_person` CROSS JOIN UNNEST(person_ids) AS person_id WHERE vid = '12-111803962-G-A')
"
person_non_rs671_path <- file.path(
  Sys.getenv("WORKSPACE_BUCKET"),
  "bq_exports",
  Sys.getenv("OWNER_EMAIL"),
  "person_demographics_non_rs671",
  "person_demographics_non_rs671_*.csv")

message(str_glue('Exporting person data for non-rs671 cohort to {person_non_rs671_path}.'))

bq_table_save(
  bq_dataset_query(Sys.getenv("WORKSPACE_CDR"), dataset_non_rs671_person_sql, billing = Sys.getenv("GOOGLE_PROJECT")),
  person_non_rs671_path,
  destination_format = "CSV")


# --- 1e. Read Exported CSVs into R Dataframes ---
read_bq_export_from_workspace_bucket <- function(export_path) {
  col_types <- cols(.default = col_character())
  bind_rows(
    map(system2('gsutil', args = c('ls', export_path), stdout = TRUE, stderr = TRUE),
        function(csv) {
          message(str_glue('Loading {csv}.'))
          read_csv(pipe(str_glue('gsutil cat {csv}')), col_types = col_types, show_col_types = FALSE)
        }))
}

dataset_non_rs671_survey_df <- read_bq_export_from_workspace_bucket(survey_non_rs671_path)
dataset_non_rs671_person_df <- read_bq_export_from_workspace_bucket(person_non_rs671_path)


#-------------------------------------------------------------------------------
# SECTION 2: DATA ANALYSIS & TABLE GENERATION
#-------------------------------------------------------------------------------

# --- 2a. DATA PREPARATION: Join the dataframes ---
# Convert concept IDs to numeric for joining
dataset_non_rs671_person_df <- dataset_non_rs671_person_df %>%
  mutate(across(c(person_id, race_concept_id, ethnicity_concept_id), as.numeric))

dataset_non_rs671_survey_df <- dataset_non_rs671_survey_df %>%
  mutate(person_id = as.numeric(person_id))

# Join the survey data with the person (demographics) data.
message("Joining datasets in R...")
combined_data <- inner_join(dataset_non_rs671_person_df, dataset_non_rs671_survey_df, by = "person_id")
message("Join complete.")

# --- 2b. DATA PROCESSING: Dynamically prepare the data for table creation ---
processed_data <- combined_data %>%
  mutate(
    race_ethnicity = case_when(
      ethnicity_concept_id == 38003563 ~ "Hispanic or Latino",
      race_concept_id == 8527 ~ "White",
      race_concept_id == 8516 ~ "Black or African American",
      race_concept_id == 8515 ~ "Asian",
      race_concept_id == 8522 ~ "American Indian or Alaska Native",
      race_concept_id == 8657 ~ "Native Hawaiian or Other Pacific Islander",
      race_concept_id == 2000000001 ~ "More than one race",
      TRUE ~ "Other/Unknown"
    ),
    category = case_when(
      grepl("In your entire life", question) ~ paste("Lifetime Use:", answer),
      grepl("How often do you have a drink", question) ~ paste("Frequency:", answer),
      grepl("how many drinks do you have", question) ~ paste("Quantity:", answer),
      grepl("six or more drinks", question) ~ paste("Binge (6+):", answer),
      TRUE ~ paste("Other:", answer)
    ),
    is_valid_answer = !grepl("PMI|Skip|Prefer Not To Answer", answer, ignore.case = TRUE)
  ) %>%
  filter(is_valid_answer)

# --- 2c. TABLE CONSTRUCTION: Build the final demographic summary table ---
demographic_table <- processed_data %>%
  group_by(race_ethnicity, category) %>%
  summarise(count = n(), .groups = 'drop') %>%
  pivot_wider(
    names_from = race_ethnicity,
    values_from = count,
    values_fill = 0
  )

# Dynamically order the rows to group questions together logically.
category_order <- c(
  "Lifetime Use: Alcohol Participant: Yes",
  "Frequency: Drink Frequency Past Year: Monthly Or Less",
  "Frequency: 2 to 4 times a month", "Frequency: 2 to 3 times a week", "Frequency: 4 or more times a week",
  "Quantity: Average Daily Drink Count: 1 or 2", "Quantity: Average Daily Drink Count: 3 or 4",
  "Quantity: Average Daily Drink Count: 5 or 6", "Quantity: Average Daily Drink Count: 7 to 9",
  "Quantity: Average Daily Drink Count: 10 or More",
  "Binge (6+): 6 or More Drinks Occurrence: Never In Last Year",
  "Binge (6+): 6 or More Drinks Occurrence: Less Than Monthly",
  "Binge (6+): 6 or More Drinks Occurrence: Monthly"
)

final_order <- intersect(category_order, demographic_table$category)
other_answers <- setdiff(demographic_table$category, final_order)
final_order <- c(final_order, other_answers)

demographic_table <- demographic_table %>%
  slice(match(final_order, category))

# --- 2d. ADD TOTALS & DISPLAY: Finalize and print the table ---

table_with_totals <- demographic_table %>%
  adorn_totals("row", name = "Total Participants") %>%
  adorn_totals("col", name = "Total (All Groups)")

print("Comprehensive Demographic Table of Alcohol Consumption (Non-rs671 Cohort)")
print(table_with_totals)
write.csv(table_with_totals, "non_rs671_alcohol_demographics.csv")
