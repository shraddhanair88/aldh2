# --- 1a. Setup: Load all necessary libraries ---
# Make sure you have these packages installed:
# install.packages(c("tidyverse", "bigrquery", "janitor"))

library(tidyverse)
library(bigrquery)
library(janitor)

# --- 1b. Define Target Questions ---
# Define the concept IDs for all alcohol questions and the race/sub-race questions.
target_question_concept_ids <- c(
  1586198, # In your entire life, have you had at least 1 drink...
  1586201, # How often did you have a drink containing alcohol in the past year?
  1586207, # On a typical day when you drink, how many drinks do you have?
  1586213, # How often did you have six or more drinks on one occasion...
  1586140, # The Basics race question
  1586151  # The Basics Asian sub-race question
)

# --- 1c. Load Filtered Survey Data for Non-rs671 Cohort ---
# This query exports the target alcohol and race questions for participants WITHOUT the rs671 variant.
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
# SECTION 2: DATA ANALYSIS & TABLE GENERATION (DISAGGREGATED ASIANS)
#-------------------------------------------------------------------------------

# --- 2a. DATA PREPARATION: Create a clean cohort of Asian subgroups ---

# First, get the main demographic data for the cohort.
person_df <- dataset_non_rs671_person_df %>%
  mutate(across(c(person_id, race_concept_id), as.numeric))

# Next, identify the person_ids for those in the cohort whose primary race is Asian (8515).
non_rs671_asian_person_ids <- person_df %>%
  filter(race_concept_id == 8515) %>%
  select(person_id)

# Now, from the full survey data, get the detailed, self-reported race answers
# ONLY for these Asian participants.
asian_subgroup_lookup <- dataset_non_rs671_survey_df %>%
  mutate(person_id = as.numeric(person_id)) %>%
  inner_join(non_rs671_asian_person_ids, by = "person_id") %>%
  # Target the specific Asian subgroup question (1586151) or answers
  filter(grepl("Asian Specific:", answer, ignore.case = TRUE)) %>%
  # Clean up the subgroup name to be used as a header.
  mutate(detailed_race = str_replace(answer, "Asian Specific: ", "")) %>%
  select(person_id, detailed_race) %>%
  distinct() # Ensure one row per person

# --- 2b. Prepare Alcohol Data and Join ---

# Get only the alcohol-related answers from the survey data.
alcohol_survey_data <- dataset_non_rs671_survey_df %>%
  filter(question_concept_id %in% as.character(target_question_concept_ids[1:4])) %>%
  mutate(person_id = as.numeric(person_id))

# Join the final Asian subgroup cohort with their alcohol answers.
# This is our final, clean dataset for analysis.
combined_data <- asian_subgroup_lookup %>%
  inner_join(alcohol_survey_data, by = "person_id")

# --- 2c. DATA PROCESSING: Create final categories for the table ---
processed_data <- combined_data %>%
  mutate(
    # The race_ethnicity is now guaranteed to be a specific subgroup from the 'detailed_race' column.
    race_ethnicity = detailed_race,
    # Use question_concept_id for robust category creation.
    category = case_when(
      question_concept_id == "1586198" ~ paste("Lifetime Use:", answer),
      question_concept_id == "1586201" ~ paste("Frequency:", answer),
      question_concept_id == "1586207" ~ paste("Quantity:", answer),
      question_concept_id == "1586213" ~ paste("Binge (6+):", answer),
      TRUE ~ "Other" # This should not be hit
    ),
    is_valid_answer = !grepl("PMI|Skip|Prefer Not To Answer", answer, ignore.case = TRUE)
  ) %>%
  filter(is_valid_answer)

# --- 2d. TABLE CONSTRUCTION: Build the final demographic summary table ---
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
  # Lifetime Use
  "Lifetime Use: Alcohol Participant: Yes",
  # Frequency
  "Frequency: Drink Frequency Past Year: Monthly Or Less",
  "Frequency: 2 to 4 times a month", "Frequency: 2 to 3 times a week", "Frequency: 4 or more times a week",
  # Quantity
  "Quantity: Average Daily Drink Count: 1 or 2", "Quantity: Average Daily Drink Count: 3 or 4",
  "Quantity: Average Daily Drink Count: 5 or 6", "Quantity: Average Daily Drink Count: 7 to 9",
  "Quantity: Average Daily Drink Count: 10 or More",
  # Binge
  "Binge (6+): 6 or More Drinks Occurrence: Never In Last Year",
  "Binge (6+): 6 or More Drinks Occurrence: Less Than Monthly",
  "Binge (6+): 6 or More Drinks Occurrence: Monthly"
)

final_order <- intersect(category_order, demographic_table$category)
other_answers <- setdiff(demographic_table$category, final_order)
final_order <- c(final_order, other_answers)

demographic_table <- demographic_table %>%
  slice(match(final_order, category))

# --- 2e. ADD TOTALS & DISPLAY: Finalize and print the table ---
table_with_totals <- demographic_table %>%
  adorn_totals("row", name = "Total Participants") %>%
  adorn_totals("col", name = "Total (All Groups)")

print("Comprehensive Demographic Table of Alcohol Consumption for Non-rs671 Cohort (Asian Subgroups)")
print(table_with_totals)
write.csv(table_with_totals, "non_rs671_alcohol_demographics_asian_subgroups.csv")
