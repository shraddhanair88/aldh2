library(tidyverse)
library(bigrquery)

# This query represents dataset "hasgeneticdatascratch" for domain "person" and was generated for All of Us Controlled Tier Dataset v8
dataset_36588778_person_sql <- paste("
    SELECT
        person.person_id,
        p_gender_concept.concept_name as gender,
        person.birth_datetime as date_of_birth,
        person.race_concept_id,
        person.ethnicity_concept_id,
        p_sex_at_birth_concept.concept_name as sex_at_birth,
        p_self_reported_category_concept.concept_name as self_reported_category 
    FROM
        `person` person 
    LEFT JOIN
        `concept` p_gender_concept 
            ON person.gender_concept_id = p_gender_concept.concept_id 
    LEFT JOIN
        `concept` p_sex_at_birth_concept 
            ON person.sex_at_birth_concept_id = p_sex_at_birth_concept.concept_id 
    LEFT JOIN
        `concept` p_self_reported_category_concept 
            ON person.self_reported_category_concept_id = p_self_reported_category_concept.concept_id  
    WHERE
        person.PERSON_ID IN (SELECT
            distinct person_id  
        FROM
            `cb_search_person` cb_search_person  
        WHERE
            cb_search_person.person_id IN (SELECT
                person_id 
            FROM
                `cb_search_person` p 
            WHERE
                has_whole_genome_variant = 1 
            UNION
            DISTINCT SELECT
                person_id 
            FROM
                `cb_search_person` p 
            WHERE
                has_lr_whole_genome_variant = 1 
            UNION
            DISTINCT SELECT
                person_id 
            FROM
                `cb_search_person` p 
            WHERE
                has_array_data = 1 
            UNION
            DISTINCT SELECT
                person_id 
            FROM
                `cb_search_person` p 
            WHERE
                has_structural_variant_data = 1 ) )", sep="")

# Formulate a Cloud Storage destination path for the data exported from BigQuery.
# NOTE: By default data exported multiple times on the same day will overwrite older copies.
#       But data exported on a different days will write to a new location so that historical
#       copies can be kept as the dataset definition is changed.
person_36588778_path <- file.path(
  Sys.getenv("WORKSPACE_BUCKET"),
  "bq_exports",
  Sys.getenv("OWNER_EMAIL"),
  strftime(lubridate::now(), "%Y%m%d"),  # Comment out this line if you want the export to always overwrite.
  "person_36588778",
  "person_36588778_*.csv")
message(str_glue('The data will be written to {person_36588778_path}. Use this path when reading ',
                 'the data into your notebooks in the future.'))

# Perform the query and export the dataset to Cloud Storage as CSV files.
# NOTE: You only need to run `bq_table_save` once. After that, you can
#       just read data from the CSVs in Cloud Storage.
bq_table_save(
  bq_dataset_query(Sys.getenv("WORKSPACE_CDR"), dataset_36588778_person_sql, billing = Sys.getenv("GOOGLE_PROJECT")),
  person_36588778_path,
  destination_format = "CSV")


# Read the data directly from Cloud Storage into memory.
# NOTE: Alternatively you can `gsutil -m cp {person_36588778_path}` to copy these files
#       to the Jupyter disk.
read_bq_export_from_workspace_bucket <- function(export_path) {
  col_types <- cols(gender = col_character(), race = col_character(), ethnicity = col_character(), sex_at_birth = col_character(), self_reported_category = col_character())
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
dataset_36588778_person_df <- read_bq_export_from_workspace_bucket(person_36588778_path)

dim(dataset_36588778_person_df)

head(dataset_36588778_person_df, 5)
library(tidyverse)
library(bigrquery)

# This query represents dataset "HasGeneticDataSupplementalandMainTables" for domain "survey" and was generated for All of Us Controlled Tier Dataset v8
dataset_37233267_survey_sql <- paste("
    SELECT
        answer.person_id,
        answer.question,
        answer.answer  
    FROM
        `ds_survey` answer   
    WHERE
        (
            question_concept_id IN (1586151, 1586198, 1586201, 1586207, 1586213)
        )  
        AND (
            answer.PERSON_ID IN (SELECT
                distinct person_id  
            FROM
                `cb_search_person` cb_search_person  
            WHERE
                cb_search_person.person_id IN (SELECT
                    person_id 
                FROM
                    `cb_search_person` p 
                WHERE
                    has_whole_genome_variant = 1 
                UNION
                DISTINCT SELECT
                    person_id 
                FROM
                    `cb_search_person` p 
                WHERE
                    has_lr_whole_genome_variant = 1 
                UNION
                DISTINCT SELECT
                    person_id 
                FROM
                    `cb_search_person` p 
                WHERE
                    has_array_data = 1 
                UNION
                DISTINCT SELECT
                    person_id 
                FROM
                    `cb_search_person` p 
                WHERE
                    has_structural_variant_data = 1 ) )
            )", sep="")

# Formulate a Cloud Storage destination path for the data exported from BigQuery.
# NOTE: By default data exported multiple times on the same day will overwrite older copies.
#       But data exported on a different days will write to a new location so that historical
#       copies can be kept as the dataset definition is changed.
survey_37233267_path <- file.path(
  Sys.getenv("WORKSPACE_BUCKET"),
  "bq_exports",
  Sys.getenv("OWNER_EMAIL"),
  strftime(lubridate::now(), "%Y%m%d"),  # Comment out this line if you want the export to always overwrite.
  "survey_37233267",
  "survey_37233267_*.csv")
message(str_glue('The data will be written to {survey_37233267_path}. Use this path when reading ',
                 'the data into your notebooks in the future.'))

# Perform the query and export the dataset to Cloud Storage as CSV files.
# NOTE: You only need to run `bq_table_save` once. After that, you can
#       just read data from the CSVs in Cloud Storage.
bq_table_save(
  bq_dataset_query(Sys.getenv("WORKSPACE_CDR"), dataset_37233267_survey_sql, billing = Sys.getenv("GOOGLE_PROJECT")),
  survey_37233267_path,
  destination_format = "CSV")


# Read the data directly from Cloud Storage into memory.
# NOTE: Alternatively you can `gsutil -m cp {survey_37233267_path}` to copy these files
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
dataset_37233267_survey_df <- read_bq_export_from_workspace_bucket(survey_37233267_path)

dim(dataset_37233267_survey_df)

head(dataset_37233267_survey_df, 5)

nrow(dataset_37233267_survey_df)

#rename for cleanliness 
finalcohort <- dataset_36588778_person_df
dim(finalcohort)

#load in death data so we can remove deceased participants
library(bigrquery)
library(tidyverse)
download_data <- function(query){
  tb <- bq_project_query(Sys.getenv('GOOGLE_PROJECT'), query = str_glue(query))
  bq_table_download(tb,bigint = "integer64")
}
dataset <- Sys.getenv('WORKSPACE_CDR')
death_data = download_data("SELECT distinct person_id, death_date
                              , death_type_concept_id, cause_concept_id, primary_death_record, src_id
                            FROM `{dataset}.aou_death`")
head(death_data)

#check for overlap with deathdata
intersect(finalcohort$person_id, death_data$person_id) 

#check there are no duplicate rows
finalcohort %>%
  count(person_id) %>%
  filter(n > 1)

#how many people are in the dataset = our initial cohort size
nrow(finalcohort) #447281

#name racial/ethnic categories
finalcohort <- finalcohort %>%
  mutate(
    race_ethnicity = case_when(
      ethnicity_concept_id == 38003563 ~ "Hispanic or Latino",
      race_concept_id == 8527 ~ "White",
      race_concept_id == 8516 ~ "Black or African American",
      race_concept_id == 8515 ~ "Asian",
      race_concept_id == 8522 ~ "American Indian or Alaska Native",
      race_concept_id == 8657 ~ "Native Hawaiian or Other Pacific Islander",
      race_concept_id == 2000000001 ~ "More than one race",
      TRUE ~ "Other/Unknown"))
#remove concept IDs
finalcohort <- finalcohort %>%
  select(-race_concept_id, -ethnicity_concept_id)

head(finalcohort)

#how many people are of each race?
race_counts <- finalcohort %>%
  group_by(race_ethnicity) %>%
  summarise(n_people = n_distinct(person_id)) %>% #there shouldn't be any duplicate rows but just in case
  arrange(desc(n_people))

race_counts


