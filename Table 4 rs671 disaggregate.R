# All of Us Registered Tier R Script
# All of Us Registered Tier R Script
# Title: HYBRID EFFICIENT Workflow for rs671 Cohort with Disaggregated Asians
#
# Description:
# This script uses a hybrid approach to balance efficiency and environment stability.
# It exports only the necessary data from BigQuery for the rs671 cohort to CSVs
# and then loads them into R for analysis. This version disaggregates the
# Asian race category into specific subgroups, creates a summary table,
# visualizes the results, and runs a multinomial regression.

#-------------------------------------------------------------------------------
# SECTION 1: EFFICIENT DATA EXPORT & LOADING FOR RS671 COHORT
#-------------------------------------------------------------------------------

# --- 1a. Setup: Load all necessary libraries ---
# Make sure you have these packages installed:
# install.packages(c("tidyverse", "bigrquery", "janitor", "nnet"))

library(tidyverse)
library(bigrquery)
library(janitor)
library(nnet)

# --- 1b. Define Target Questions ---
# Define the concept IDs for all alcohol questions you want to include.
target_question_concept_ids <- c(
  1586198, # In your entire life, have you had at least 1 drink...
  1586201, # How often did you have a drink containing alcohol in the past year?
  1586207, # On a typical day when you drink, how many drinks do you have?
  1586213, # How often did you have six or more drinks on one occasion...
  1586140, # The Basics race question
  1586151  # The Basics Asian sub-race question
)

# --- 1c. Load Filtered Survey Data for rs671 Cohort ---
# This query exports only the target alcohol questions for the rs671 cohort.
dataset_rs671_survey_sql <- paste(
  "SELECT person_id, question_concept_id, question, answer FROM `ds_survey` WHERE question_concept_id IN (",
  paste(target_question_concept_ids, collapse = ", "),
  ") AND PERSON_ID IN (SELECT distinct person_id FROM `cb_search_person` WHERE person_id IN (SELECT person_id FROM `cb_variant_to_person` CROSS JOIN UNNEST(person_ids) AS person_id WHERE vid = '12-111803962-G-A'))"
)

survey_rs671_path <- file.path(
  Sys.getenv("WORKSPACE_BUCKET"),
  "bq_exports",
  Sys.getenv("OWNER_EMAIL"),
  "filtered_survey_alcohol_rs671",
  "filtered_survey_alcohol_rs671_*.csv")

message(str_glue('Exporting filtered survey data for rs671 cohort to {survey_rs671_path}.'))

bq_table_save(
  bq_dataset_query(Sys.getenv("WORKSPACE_CDR"), dataset_rs671_survey_sql, billing = Sys.getenv("GOOGLE_PROJECT")),
  survey_rs671_path,
  destination_format = "CSV")

# --- 1d. Load Person Data for rs671 Cohort ---
# This query exports demographic data only for the rs671 cohort.
dataset_rs671_person_sql <- "
    SELECT person_id, race_concept_id, ethnicity_concept_id
    FROM `person`
    WHERE PERSON_ID IN (SELECT distinct person_id FROM `cb_search_person` WHERE person_id IN (SELECT person_id FROM `cb_variant_to_person` CROSS JOIN UNNEST(person_ids) AS person_id WHERE vid = '12-111803962-G-A'))
"
person_rs671_path <- file.path(
  Sys.getenv("WORKSPACE_BUCKET"),
  "bq_exports",
  Sys.getenv("OWNER_EMAIL"),
  "person_demographics_rs671",
  "person_demographics_rs671_*.csv")

message(str_glue('Exporting person data for rs671 cohort to {person_rs671_path}.'))

bq_table_save(
  bq_dataset_query(Sys.getenv("WORKSPACE_CDR"), dataset_rs671_person_sql, billing = Sys.getenv("GOOGLE_PROJECT")),
  person_rs671_path,
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

dataset_rs671_survey_df <- read_bq_export_from_workspace_bucket(survey_rs671_path)
dataset_rs671_person_df <- read_bq_export_from_workspace_bucket(person_rs671_path)


#-------------------------------------------------------------------------------
# SECTION 2: DATA ANALYSIS & TABLE GENERATION (REVISED LOGIC)
#-------------------------------------------------------------------------------

# --- 2a. DATA PREPARATION: Create a clean cohort of Asian subgroups ---
person_df <- dataset_rs671_person_df %>%
  mutate(across(c(person_id, race_concept_id), as.numeric))

rs671_asian_person_ids <- person_df %>%
  filter(race_concept_id == 8515) %>%
  select(person_id)

asian_subgroup_lookup <- dataset_rs671_survey_df %>%
  mutate(person_id = as.numeric(person_id)) %>%
  inner_join(rs671_asian_person_ids, by = "person_id") %>%
  filter(grepl("Asian Specific:", answer, ignore.case = TRUE)) %>%
  mutate(detailed_race = str_replace(answer, "Asian Specific: ", "")) %>%
  select(person_id, detailed_race) %>%
  distinct()

# --- 2b. Prepare Alcohol Data and Join ---
alcohol_survey_data <- dataset_rs671_survey_df %>%
  filter(question_concept_id %in% as.character(target_question_concept_ids[1:4])) %>%
  mutate(person_id = as.numeric(person_id))

combined_data <- asian_subgroup_lookup %>%
  inner_join(alcohol_survey_data, by = "person_id")

# --- 2c. DATA PROCESSING: Create final categories for the table ---
processed_data <- combined_data %>%
  mutate(
    race_ethnicity = detailed_race,
    clean_answer = str_remove_all(answer, "Drink Frequency Past Year: |Average Daily Drink Count: |6 or More Drinks Occurrence: |Alcohol Participant: "),
    category = case_when(
      question_concept_id == "1586198" ~ paste("Lifetime Use:", clean_answer),
      question_concept_id == "1586201" ~ paste("Frequency:", clean_answer),
      question_concept_id == "1586207" ~ paste("Quantity:", clean_answer),
      question_concept_id == "1586213" ~ paste("Binge (6+):", clean_answer),
      TRUE ~ "Other"
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

category_order <- c(
  "Lifetime Use: Yes",
  "Frequency: Monthly Or Less",
  "Frequency: 2 to 4 times a month", "Frequency: 2 to 3 times a week", "Frequency: 4 or more times a week",
  "Quantity: 1 or 2", "Quantity: 3 or 4", "Quantity: 5 or 6", "Quantity: 7 to 9", "Quantity: 10 or More",
  "Binge (6+): Never In Last Year", "Binge (6+): Less Than Monthly", "Binge (6+): Monthly"
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

print("Comprehensive Demographic Table of Alcohol Consumption for rs671 Cohort (Asian Subgroups)")
print(table_with_totals)
write.csv(table_with_totals, "rs671_alcohol_demographics_asian_subgroups.csv")

#-------------------------------------------------------------------------------
# SECTION 3: DATA VISUALIZATION
#-------------------------------------------------------------------------------
plot_data <- demographic_table %>%
  pivot_longer(cols = -category, names_to = "race_ethnicity", values_to = "count") %>%
  separate(category, into = c("question_group", "answer"), sep = ": ", remove = FALSE, extra = "merge")

frequency_data <- plot_data %>% filter(question_group == "Frequency")

frequency_plot <- ggplot(frequency_data, aes(x = race_ethnicity, y = count, fill = answer)) +
  geom_bar(stat = "identity", position = "fill") +
  scale_y_continuous(labels = scales::percent) +
  labs(
    title = "Proportional Alcohol Consumption Frequency by Asian Subgroup (rs671 Cohort)",
    x = "Asian Subgroup",
    y = "Proportion of Participants",
    fill = "Consumption Frequency"
  ) +
  theme_minimal() +
  theme(axis.text.x = element_text(angle = 45, hjust = 1))

print(frequency_plot)
ggsave("alcohol_frequency_by_asian_subgroup_rs671.png", plot = frequency_plot, width = 12, height = 8)
message("Visualization saved as alcohol_frequency_by_asian_subgroup_rs671.png")

#-------------------------------------------------------------------------------
# SECTION 4: MULTINOMIAL REGRESSION ANALYSIS
#-------------------------------------------------------------------------------

# --- 4a. Prepare Data for Regression ---
regression_base <- processed_data %>%
  select(person_id, race_ethnicity, question_concept_id, clean_answer) %>%
  pivot_wider(names_from = question_concept_id, values_from = clean_answer, values_fn = list) %>%
  unnest(cols = everything(), keep_empty = TRUE)

regression_data <- regression_base %>%
  mutate(
    drinking_frequency = case_when(
      `1586198` == "No" ~ "Nondrinker",
      `1586201` == "Never in past year" ~ "Nondrinker",
      !is.na(`1586201`) ~ `1586201`,
      TRUE ~ NA_character_
    )
  ) %>%
  filter(!is.na(drinking_frequency))

# Set reference levels. We'll use the largest subgroup as the reference.
# Find the largest subgroup to use as the reference
largest_subgroup <- regression_data %>%
  count(race_ethnicity, sort = TRUE) %>%
  slice(1) %>%
  pull(race_ethnicity)

message(paste("Using", largest_subgroup, "as the reference group for regression."))

regression_data$drinking_frequency <- as.factor(regression_data$drinking_frequency)
regression_data$drinking_frequency <- relevel(regression_data$drinking_frequency, ref = "Nondrinker")
regression_data$race_ethnicity <- as.factor(regression_data$race_ethnicity)
regression_data$race_ethnicity <- relevel(regression_data$race_ethnicity, ref = largest_subgroup)

# --- 4b. Fit the Multinomial Regression Model ---
message("Fitting multinomial regression model for Asian subgroups...")
model <- multinom(drinking_frequency ~ race_ethnicity, data = regression_data)
message("Model fitting complete.")

# --- 4c. Display and Interpret the Results ---
print(summary(model))
rrr <- exp(coef(model))
print("Relative Risk Ratios (RRR):")
print(rrr)
z_scores <- summary(model)$coefficients / summary(model)$standard.errors
p_values <- (1 - pnorm(abs(z_scores), 0, 1)) * 2
print("P-values:")
print(p_values)
