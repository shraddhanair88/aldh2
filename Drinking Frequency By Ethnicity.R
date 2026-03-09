library(tidyverse)
library(bigrquery)

# This query represents dataset "asianethnicity" for domain "survey" and was generated for All of Us Controlled Tier Dataset v8
dataset_04669370_survey_sql <- paste("
    SELECT
        answer.person_id,
        answer.answer  
    FROM
        `ds_survey` answer   
    WHERE
        (
            question_concept_id IN (1586151)
        )", sep="")

# Formulate a Cloud Storage destination path for the data exported from BigQuery.
# NOTE: By default data exported multiple times on the same day will overwrite older copies.
#       But data exported on a different days will write to a new location so that historical
#       copies can be kept as the dataset definition is changed.
survey_04669370_path <- file.path(
  Sys.getenv("WORKSPACE_BUCKET"),
  "bq_exports",
  Sys.getenv("OWNER_EMAIL"),
  strftime(lubridate::now(), "%Y%m%d"),  # Comment out this line if you want the export to always overwrite.
  "survey_04669370",
  "survey_04669370_*.csv")
message(str_glue('The data will be written to {survey_04669370_path}. Use this path when reading ',
                 'the data into your notebooks in the future.'))

# Perform the query and export the dataset to Cloud Storage as CSV files.
# NOTE: You only need to run `bq_table_save` once. After that, you can
#       just read data from the CSVs in Cloud Storage.
bq_table_save(
  bq_dataset_query(Sys.getenv("WORKSPACE_CDR"), dataset_04669370_survey_sql, billing = Sys.getenv("GOOGLE_PROJECT")),
  survey_04669370_path,
  destination_format = "CSV")


# Read the data directly from Cloud Storage into memory.
# NOTE: Alternatively you can `gsutil -m cp {survey_04669370_path}` to copy these files
#       to the Jupyter disk.
read_bq_export_from_workspace_bucket <- function(export_path) {
  col_types <- cols(survey = col_character(), question = col_character(), answer = col_character())
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
dataset_04669370_survey_df <- read_bq_export_from_workspace_bucket(survey_04669370_path)

dim(dataset_04669370_survey_df)

head(dataset_04669370_survey_df, 5)

asianethnicity <- dataset_04669370_survey_df 
nrow(asianethnicity) #some cleanup required

#people who selected multiple ethnicities show up 2+ times, so we'll delete all multi-select ppl
asianethnicity %>%
  count(person_id) %>%
  filter(n > 1) 

#1413 people need to be removed (>1413 rows from asianethnicity)
asianethnicity <- asianethnicity %>%
  group_by(person_id) %>%
  filter(n() == 1) %>%
  ungroup() #down to 26707!

#Check again
asianethnicity %>%
  count(person_id) %>%
  filter(n > 1) #works

# Remove PMI / Skip / empty / "NA" responses
drink_freq_rs671_asian <- drink_freq_rs671_asian %>%
  filter(!answer %in% c("PMI: Prefer Not To Answer", "PMI: Skip", "", "NA"))

# Make person_id the same type
asianethnicity <- asianethnicity %>% mutate(person_id = as.character(person_id))
drink_freq_rs671_asian <- drink_freq_rs671_asian %>% mutate(person_id = as.character(person_id))

# Left join
rs671_asian_ethnicity <- left_join(
  drink_freq_rs671_asian,
  asianethnicity,
  by = "person_id"
)

# Remove rows with missing drink response or ethnicity
rs671_asian_ethnicity_clean <- rs671_asian_ethnicity %>%
  filter(!is.na(answer.x), !is.na(answer.y))

drink_frequency_order <- c(
  "Drink Frequency Past Year: Never",
  "Drink Frequency Past Year: Monthly Or Less",
  "Drink Frequency Past Year: 2 to 4 Per Month",
  "Drink Frequency Past Year: 2 to 3 Per Week",
  "Drink Frequency Past Year: 4 or More Per Week"
)

##Make tables!

#Chinese
asian_chinese_drink_freq_carrier <- rs671_asian_ethnicity_clean %>%
  filter(answer.y == "Asian Specific: Chinese") %>%
  mutate(answer.x = factor(answer.x, levels = drink_frequency_order)) %>%
  group_by(answer.x) %>%
  summarise(count = n(), .groups = "drop") %>%
  mutate(percent = 100 * count / sum(count),
         value = paste0(count, " (", sprintf("%.2f", percent), "%)")) %>%
  select(answer = answer.x, `Asian Specific: Chinese` = value)

#Japanese
asian_japanese_drink_freq_carrier <- rs671_asian_ethnicity_clean %>%
  filter(answer.y == "Asian Specific: Japanese") %>%
  mutate(answer.x = factor(answer.x, levels = drink_frequency_order)) %>%
  group_by(answer.x) %>%
  summarise(count = n(), .groups = "drop") %>%
  mutate(percent = 100 * count / sum(count),
         value = paste0(count, " (", sprintf("%.2f", percent), "%)")) %>%
  select(answer = answer.x, `Asian Specific: Japanese` = value)

#Korean
asian_korean_drink_freq_carrier <- rs671_asian_ethnicity_clean %>%
  filter(answer.y == "Asian Specific: Korean") %>%
  mutate(answer.x = factor(answer.x, levels = drink_frequency_order)) %>%
  group_by(answer.x) %>%
  summarise(count = n(), .groups = "drop") %>%
  mutate(percent = 100 * count / sum(count),
         value = paste0(count, " (", sprintf("%.2f", percent), "%)")) %>%
  select(answer = answer.x, `Asian Specific: Korean` = value)

#Filipino
asian_filipino_drink_freq_carrier <- rs671_asian_ethnicity_clean %>%
  filter(answer.y == "Asian Specific: Filipino") %>%
  mutate(answer.x = factor(answer.x, levels = drink_frequency_order)) %>%
  group_by(answer.x) %>%
  summarise(count = n(), .groups = "drop") %>%
  mutate(percent = 100 * count / sum(count),
         value = paste0(count, " (", sprintf("%.2f", percent), "%)")) %>%
  select(answer = answer.x, `Asian Specific: Filipino` = value)

#Vietnamese
asian_vietnamese_drink_freq_carrier <- rs671_asian_ethnicity_clean %>%
  filter(answer.y == "Asian Specific: Vietnamese") %>%
  mutate(answer.x = factor(answer.x, levels = drink_frequency_order)) %>%
  group_by(answer.x) %>%
  summarise(count = n(), .groups = "drop") %>%
  mutate(percent = 100 * count / sum(count),
         value = paste0(count, " (", sprintf("%.2f", percent), "%)")) %>%
  select(answer = answer.x, `Asian Specific: Vietnamese` = value)

#What about non-carriers?
drink_freq_nors671_asian <- drink_freq_nors671_asian %>%
  filter(!answer %in% c("PMI: Prefer Not To Answer", "PMI: Skip", "", "NA"))

asianethnicity <- asianethnicity %>% mutate(person_id = as.character(person_id))
drink_freq_nors671_asian <- drink_freq_nors671_asian %>% mutate(person_id = as.character(person_id))

nors671_asian_ethnicity <- left_join(
  drink_freq_nors671_asian,
  asianethnicity,
  by = "person_id"
)

nors671_asian_ethnicity_clean <- nors671_asian_ethnicity %>%
  filter(!is.na(answer.x), !is.na(answer.y))

#Chinese
asian_chinese_drink_freq_noncarrier <- nors671_asian_ethnicity_clean %>%
  filter(answer.y == "Asian Specific: Chinese") %>%
  mutate(answer.x = factor(answer.x, levels = drink_frequency_order)) %>%
  group_by(answer.x) %>%
  summarise(count = n(), .groups = "drop") %>%
  mutate(percent = 100 * count / sum(count),
         value = paste0(count, " (", sprintf("%.2f", percent), "%)")) %>%
  select(answer = answer.x, `Asian Specific: Chinese` = value)

#Japanese
asian_japanese_drink_freq_noncarrier <- nors671_asian_ethnicity_clean %>%
  filter(answer.y == "Asian Specific: Japanese") %>%
  mutate(answer.x = factor(answer.x, levels = drink_frequency_order)) %>%
  group_by(answer.x) %>%
  summarise(count = n(), .groups = "drop") %>%
  mutate(percent = 100 * count / sum(count),
         value = paste0(count, " (", sprintf("%.2f", percent), "%)")) %>%
  select(answer = answer.x, `Asian Specific: Japanese` = value)

#Korean
asian_korean_drink_freq_noncarrier <- nors671_asian_ethnicity_clean %>%
  filter(answer.y == "Asian Specific: Korean") %>%
  mutate(answer.x = factor(answer.x, levels = drink_frequency_order)) %>%
  group_by(answer.x) %>%
  summarise(count = n(), .groups = "drop") %>%
  mutate(percent = 100 * count / sum(count),
         value = paste0(count, " (", sprintf("%.2f", percent), "%)")) %>%
  select(answer = answer.x, `Asian Specific: Korean` = value)

#Filipino
asian_filipino_drink_freq_noncarrier <- nors671_asian_ethnicity_clean %>%
  filter(answer.y == "Asian Specific: Filipino") %>%
  mutate(answer.x = factor(answer.x, levels = drink_frequency_order)) %>%
  group_by(answer.x) %>%
  summarise(count = n(), .groups = "drop") %>%
  mutate(percent = 100 * count / sum(count),
         value = paste0(count, " (", sprintf("%.2f", percent), "%)")) %>%
  select(answer = answer.x, `Asian Specific: Filipino` = value)

#Vietnamese
asian_vietnamese_drink_freq_noncarrier <- nors671_asian_ethnicity_clean %>%
  filter(answer.y == "Asian Specific: Vietnamese") %>%
  mutate(answer.x = factor(answer.x, levels = drink_frequency_order)) %>%
  group_by(answer.x) %>%
  summarise(count = n(), .groups = "drop") %>%
  mutate(percent = 100 * count / sum(count),
         value = paste0(count, " (", sprintf("%.2f", percent), "%)")) %>%
  select(answer = answer.x, `Asian Specific: Vietnamese` = value)

#Hhow many rs671 people are of each ethnic subgroup?
library(dplyr)

# Make person_id the same type
rs671_people <- rs671_people %>% mutate(person_id = as.character(person_id))
asianethnicity <- asianethnicity %>% mutate(person_id = as.character(person_id))

# Filter rs671_people to only Asians
rs671_people_asian_only <- rs671_people %>% 
  filter(race_ethnicity == "Asian")

# Remove people who selected multiple Asian ethnicities
asianethnicity_clean <- asianethnicity %>%
  group_by(person_id) %>%
  filter(n() == 1) %>%
  ungroup()

# Join rs671_people_asian_only with ethnicity table
rs671_people_asian <- left_join(
  rs671_people_asian_only,
  asianethnicity_clean,
  by = "person_id"
)
nrow(rs671_people_asian) #3168

# Remove rows with missing ethnicity
rs671_people_asian_clean <- rs671_people_asian %>%
  filter(!is.na(answer))

# Verify no duplicated person_ids remain
rs671_people_asian_clean %>%
  count(person_id) %>%
  filter(n > 1)

# Final row count
nrow(rs671_people_asian_clean) #3039, not everyone who marked "Asian" specified a particular ethnicity so this table is <3168

#Make table with percentages
# Count + percentage table for Asian ethnicities
rs671_asian_ethnicity_table <- rs671_people_asian_clean %>%
  group_by(answer) %>%
  summarise(count = n(), .groups = "drop") %>%
  mutate(percent = 100 * count / sum(count)) %>%
  arrange(desc(count))  # optional: sort by count

# View table
rs671_asian_ethnicity_table

#nors671
library(dplyr)

# Make person_id the same type
no_rs671_people <- no_rs671_people %>% mutate(person_id = as.character(person_id))
asianethnicity <- asianethnicity %>% mutate(person_id = as.character(person_id))

# Filter no_rs671_people to only Asians
no_rs671_people_asian_only <- no_rs671_people %>% 
  filter(race_ethnicity == "Asian")

# Remove people who selected multiple Asian ethnicities
asianethnicity_clean <- asianethnicity %>%
  group_by(person_id) %>%
  filter(n() == 1) %>%
  ungroup()

# Join no_rs671_people_asian_only with ethnicity table
no_rs671_people_asian <- left_join(
  no_rs671_people_asian_only,
  asianethnicity_clean,
  by = "person_id"
)
nrow(no_rs671_people_asian)

# Remove rows with missing ethnicity
no_rs671_people_asian_clean <- no_rs671_people_asian %>%
  filter(!is.na(answer))

# Verify no duplicated person_ids remain
no_rs671_people_asian_clean %>%
  count(person_id) %>%
  filter(n > 1)

# Final row count
nrow(no_rs671_people_asian_clean)

# Count + percentage table for Asian ethnicities
nors671_asian_ethnicity_table <- no_rs671_people_asian_clean %>%
  group_by(answer) %>%
  summarise(count = n(), .groups = "drop") %>%
  mutate(percent = 100 * count / sum(count)) %>%
  arrange(desc(count))  # optional: sort by count

# View table
nors671_asian_ethnicity_table

nrow(no_rs671_people)