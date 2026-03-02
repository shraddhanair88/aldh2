library(tidyverse)
library(bigrquery)

# This query represents dataset "Has Genetic Data" for domain "person" and was generated for All of Us Controlled Tier Dataset v8
dataset_80892251_person_sql <- paste("
    SELECT
        person.person_id 
    FROM
        `person` person   
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
person_80892251_path <- file.path(
  Sys.getenv("WORKSPACE_BUCKET"),
  "bq_exports",
  Sys.getenv("OWNER_EMAIL"),
  strftime(lubridate::now(), "%Y%m%d"),  # Comment out this line if you want the export to always overwrite.
  "person_80892251",
  "person_80892251_*.csv")
message(str_glue('The data will be written to {person_80892251_path}. Use this path when reading ',
                 'the data into your notebooks in the future.'))

# Perform the query and export the dataset to Cloud Storage as CSV files.
# NOTE: You only need to run `bq_table_save` once. After that, you can
#       just read data from the CSVs in Cloud Storage.
bq_table_save(
  bq_dataset_query(Sys.getenv("WORKSPACE_CDR"), dataset_80892251_person_sql, billing = Sys.getenv("GOOGLE_PROJECT")),
  person_80892251_path,
  destination_format = "CSV")


# Read the data directly from Cloud Storage into memory.
# NOTE: Alternatively you can `gsutil -m cp {person_80892251_path}` to copy these files
#       to the Jupyter disk.
read_bq_export_from_workspace_bucket <- function(export_path) {
  col_types <- NULL
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
dataset_80892251_person_df <- read_bq_export_from_workspace_bucket(person_80892251_path)

dim(dataset_80892251_person_df)

head(dataset_80892251_person_df, 5)
gene <- dataset_80892251_person_df
sankeydata$gene <- ifelse(sankeydata$person_id %in% gene$person_id, 1, 0)
table(sankeydata$gene)
sankeydata_gene <- sankeydata[sankeydata$gene == 1, ]
table(sankeydata_gene)
table(sankeydata_gene$race_ethnicity)

#WITH RS671 calculations _____________________

#load in everyone with rs671 with age and sex data included
library(tidyverse)
library(bigrquery)

# This query represents dataset "rs671 variant holders - demographics and The Basics" for domain "person" and was generated for All of Us Controlled Tier Dataset v8
dataset_14969606_person_sql <- paste("
    SELECT
        person.person_id,
        p_gender_concept.concept_name as gender,
        person.birth_datetime as date_of_birth,
        p_sex_at_birth_concept.concept_name as sex_at_birth 
    FROM
        `person` person 
    LEFT JOIN
        `concept` p_gender_concept 
            ON person.gender_concept_id = p_gender_concept.concept_id 
    LEFT JOIN
        `concept` p_sex_at_birth_concept 
            ON person.sex_at_birth_concept_id = p_sex_at_birth_concept.concept_id  
    WHERE
        person.PERSON_ID IN (SELECT
            distinct person_id  
        FROM
            `cb_search_person` cb_search_person  
        WHERE
            cb_search_person.person_id IN (SELECT
                person_id 
            FROM
                `cb_variant_to_person` 
            CROSS JOIN
                UNNEST(person_ids) AS person_id 
            WHERE
                vid IN ('12-111803962-G-A') ) )", sep="")

# Formulate a Cloud Storage destination path for the data exported from BigQuery.
# NOTE: By default data exported multiple times on the same day will overwrite older copies.
#       But data exported on a different days will write to a new location so that historical
#       copies can be kept as the dataset definition is changed.
person_14969606_path <- file.path(
  Sys.getenv("WORKSPACE_BUCKET"),
  "bq_exports",
  Sys.getenv("OWNER_EMAIL"),
  strftime(lubridate::now(), "%Y%m%d"),  # Comment out this line if you want the export to always overwrite.
  "person_14969606",
  "person_14969606_*.csv")
message(str_glue('The data will be written to {person_14969606_path}. Use this path when reading ',
                 'the data into your notebooks in the future.'))

# Perform the query and export the dataset to Cloud Storage as CSV files.
# NOTE: You only need to run `bq_table_save` once. After that, you can
#       just read data from the CSVs in Cloud Storage.
bq_table_save(
  bq_dataset_query(Sys.getenv("WORKSPACE_CDR"), dataset_14969606_person_sql, billing = Sys.getenv("GOOGLE_PROJECT")),
  person_14969606_path,
  destination_format = "CSV")


# Read the data directly from Cloud Storage into memory.
# NOTE: Alternatively you can `gsutil -m cp {person_14969606_path}` to copy these files
#       to the Jupyter disk.
read_bq_export_from_workspace_bucket <- function(export_path) {
  col_types <- cols(gender = col_character(), sex_at_birth = col_character())
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
dataset_14969606_person_df <- read_bq_export_from_workspace_bucket(person_14969606_path)

dim(dataset_14969606_person_df)

head(dataset_14969606_person_df, 5)
rs671cohort <- dataset_14969606_person_df

#join and antijoin with sankeydata_gene to create 2 datasets, with rs671 and without rs671
#with rs671: (convert both person_id columns to character, then join)
library(dplyr)
withrs671 <- rs671cohort %>%
  mutate(person_id = as.character(person_id)) %>%
  left_join(
    sankeydata_gene %>% mutate(person_id = as.character(person_id)),
    by = "person_id"
  )

withrs671 <- sankeydata_gene %>%
  semi_join(rs671cohort %>% mutate(person_id = as.character(person_id)),
            by = "person_id")
dim(withrs671) #3801 people!
#why 3801? --> rs671cohort had 3,827 people in total. 26 of them aren’t in sankeydata_gene (probably passed away), so they’re excluded by semi_join().

#add in age and sex info separately
library(tidyverse)
library(bigrquery)

# This query represents dataset "everyone in all of us with sex and date of birth" for domain "person" and was generated for All of Us Controlled Tier Dataset v8
dataset_39687183_person_sql <- paste("
    SELECT
        person.person_id,
        p_gender_concept.concept_name as gender,
        person.birth_datetime as date_of_birth,
        p_sex_at_birth_concept.concept_name as sex_at_birth 
    FROM
        `person` person 
    LEFT JOIN
        `concept` p_gender_concept 
            ON person.gender_concept_id = p_gender_concept.concept_id 
    LEFT JOIN
        `concept` p_sex_at_birth_concept 
            ON person.sex_at_birth_concept_id = p_sex_at_birth_concept.concept_id", sep="")

# Formulate a Cloud Storage destination path for the data exported from BigQuery.
# NOTE: By default data exported multiple times on the same day will overwrite older copies.
#       But data exported on a different days will write to a new location so that historical
#       copies can be kept as the dataset definition is changed.
person_39687183_path <- file.path(
  Sys.getenv("WORKSPACE_BUCKET"),
  "bq_exports",
  Sys.getenv("OWNER_EMAIL"),
  strftime(lubridate::now(), "%Y%m%d"),  # Comment out this line if you want the export to always overwrite.
  "person_39687183",
  "person_39687183_*.csv")
message(str_glue('The data will be written to {person_39687183_path}. Use this path when reading ',
                 'the data into your notebooks in the future.'))

# Perform the query and export the dataset to Cloud Storage as CSV files.
# NOTE: You only need to run `bq_table_save` once. After that, you can
#       just read data from the CSVs in Cloud Storage.
bq_table_save(
  bq_dataset_query(Sys.getenv("WORKSPACE_CDR"), dataset_39687183_person_sql, billing = Sys.getenv("GOOGLE_PROJECT")),
  person_39687183_path,
  destination_format = "CSV")


# Read the data directly from Cloud Storage into memory.
# NOTE: Alternatively you can `gsutil -m cp {person_39687183_path}` to copy these files
#       to the Jupyter disk.
read_bq_export_from_workspace_bucket <- function(export_path) {
  col_types <- cols(gender = col_character(), sex_at_birth = col_character())
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
dataset_39687183_person_df <- read_bq_export_from_workspace_bucket(person_39687183_path)

dim(dataset_39687183_person_df)

head(dataset_39687183_person_df, 5)

#add to withrs671 dataframe
library(dplyr)

# Make sure person_id is character in both dataframes
withrs671 <- withrs671 %>%
  mutate(person_id = as.character(person_id))

dataset_39687183_person_df <- dataset_39687183_person_df %>%
  mutate(person_id = as.character(person_id))

# Join demographic info
withrs671 <- withrs671 %>%
  left_join(
    dataset_39687183_person_df %>%
      select(person_id, sex_at_birth, gender, date_of_birth),
    by = "person_id"
  )


#clean up age and sex data for withrs671
#group ages:
library(lubridate)

withrs671 <- withrs671 %>%
  mutate(
    # calculate age in years
    age = floor(interval(date_of_birth, Sys.Date()) / years(1)),
    
    # create age groups
    age_group = case_when(
      age >= 18 & age <= 29 ~ "18-29",
      age >= 30 & age <= 39 ~ "30-39",
      age >= 40 & age <= 49 ~ "40-49",
      age >= 50 & age <= 59 ~ "50-59",
      age >= 60 & age <= 69 ~ "60-69",
      age >= 70 & age <= 79 ~ "70-79",
      age >= 80 & age <= 89 ~ "80-89",
      age >= 90             ~ "90+",
      TRUE                  ~ NA_character_
    )
  )
#display counts and percentages
age_summary <- withrs671 %>%
  group_by(age_group) %>%
  summarise(
    n_people = n(),                   # count of people in each group
    percent = round(100 * n() / nrow(withrs671), 2)  # percentage
  ) %>%
  arrange(factor(age_group, 
                 levels = c("18-29", "30-39", "40-49", "50-59", 
                            "60-69", "70-79", "80-89", "90+")))

#clean up sex_at_birth column
unique(withrs671$sex_at_birth)

sex_summary <- withrs671 %>%
  mutate(
    sex_at_birth = case_when(
      sex_at_birth == "Male" ~ "Male",
      sex_at_birth == "Female" ~ "Female",
      TRUE ~ "Other"
    )
  ) %>%
  group_by(sex_at_birth) %>%
  summarise(
    n_people = n(),
    percent = round(100 * n() / nrow(withrs671), 2)
  ) %>%
  arrange(desc(n_people))

#WITHOUT RS671 calculations _______________
#create dataframe by joining everyone not in rs671 cohort

withoutrs671 <- sankeydata_gene %>%
  mutate(person_id = as.character(person_id)) %>%
  left_join(
    rs671cohort %>% mutate(person_id = as.character(person_id)), 
    by = "person_id",
    suffix = c(".gene", ".rs671")
  ) %>%
  # keep only rows where the person_id is NOT in rs671cohort
  filter(is.na(sex_at_birth))  # replace rs671_variant_column with any column from rs671cohort

sankeydata_gene <- sankeydata_gene %>%
  mutate(person_id = as.character(person_id))

rs671cohort <- rs671cohort %>%
  mutate(person_id = as.character(person_id))
withoutrs671 <- anti_join(sankeydata_gene, rs671cohort, by = "person_id")

#doublecheck nrows is correctly 441630 (sankeydata_gene number) minus 3827 should equal 437803 rows
dim(withoutrs671) #437829 rows, we have discrepancy of 26 people
nrow(sankeydata_gene)
n_distinct(sankeydata_gene$person_id)

nrow(rs671cohort)
n_distinct(rs671cohort$person_id)
#maybe the numeric formatting differs between datasets?
# Compare sets of IDs
setdiff(
  rs671cohort %>% mutate(person_id = as.character(person_id)) %>% pull(person_id),
  sankeydata_gene %>% mutate(person_id = as.character(person_id)) %>% pull(person_id)
) %>% head()
#returns that there are unique IDs in rs671 that are not in sankeydata_gene which is fine and expected considering we ruled out dead people
#find the unique IDs lost in the antijoin, should equal 26
missing_ids <- setdiff(
  rs671cohort %>% mutate(person_id = as.character(person_id)) %>% pull(person_id),
  sankeydata_gene %>% mutate(person_id = as.character(person_id)) %>% pull(person_id)
)
#rs671cohort: 3,827 rows total, but 26 of those don’t exist in sankeydata_gene, maybe due to passing away
#So when we removed the rs671 people using left_join(), we actually removed only 3,801 people (3,827 − 26), leaving 437,829 rows, not the expected 437,803.

length(missing_ids) #equals 26!

#we must remove this group from the withoutrs671 dataset because they have rs671, they likely just passed away

#add in age and sex info to withoutrs671 dataframe, make sure person_id is character in both frames
withoutrs671 <- withoutrs671 %>%
  mutate(person_id = as.character(person_id))

dataset_39687183_person_df <- dataset_39687183_person_df %>%
  mutate(person_id = as.character(person_id))
#join the sex and DOB columns
withoutrs671 <- withoutrs671 %>%
  left_join(
    dataset_39687183_person_df %>%
      select(person_id, sex_at_birth, gender, date_of_birth),
    by = "person_id"
  )


#continue with age and sex cleanup and breakdown:

withoutrs671 <- withoutrs671 %>%
  mutate(
    # ensure date_of_birth is Date type
    date_of_birth = as.Date(date_of_birth),
    
    # calculate age in years
    age = floor(interval(date_of_birth, Sys.Date()) / years(1)),
    
    # create age groups
    age_group = case_when(
      age >= 18 & age <= 29 ~ "18-29",
      age >= 30 & age <= 39 ~ "30-39",
      age >= 40 & age <= 49 ~ "40-49",
      age >= 50 & age <= 59 ~ "50-59",
      age >= 60 & age <= 69 ~ "60-69",
      age >= 70 & age <= 79 ~ "70-79",
      age >= 80 & age <= 89 ~ "80-89",
      age >= 90             ~ "90+",
      TRUE                  ~ NA_character_
    )
  )

age_summary_without <- withoutrs671 %>%
  group_by(age_group) %>%
  summarise(
    n_people = n(),
    percent = round(100 * n() / nrow(withoutrs671), 2)
  ) %>%
  arrange(factor(age_group, 
                 levels = c("18-29", "30-39", "40-49", "50-59",
                            "60-69", "70-79", "80-89", "90+")))

sex_summary_without <- withoutrs671 %>%
  mutate(
    sex_at_birth = case_when(
      sex_at_birth == "Male" ~ "Male",
      sex_at_birth == "Female" ~ "Female",
      TRUE ~ "Other"
    )
  ) %>%
  group_by(sex_at_birth) %>%
  summarise(
    n_people = n(),
    percent = round(100 * n() / nrow(withoutrs671), 2)
  ) %>%
  arrange(desc(n_people))

#check some totals:
#total in all summary tables should equal the 437829 number (nrows of withoutrs671) and 3827 (nrows of withrs671)

total_age_withrs671 <- sum(age_summary$n_people)
total_sex_withrs671 <- sum(sex_summary$n_people)

total_age_withoutrs671 <- sum(age_summary_without$n_people)
total_sex_withoutrs671 <- sum(sex_summary_without$n_people)

#checks out :)


#ASIANS ONLY, WITH rs671______
# Filter rs671 cohort for Asians
asians_rs671 <- withrs671 %>%
  filter(race_ethnicity == "Asian")
dim(asians_rs671) #3148 Asians in this cohort

# Age distribution
asians_rs671_age <- asians_rs671 %>%
  group_by(age_group) %>%
  summarise(
    n_people = n(),
    percent = round(100 * n() / nrow(asians_rs671), 2)
  ) %>%
  arrange(factor(age_group, 
                 levels = c("18-29", "30-39", "40-49", "50-59",
                            "60-69", "70-79", "80-89", "90+")))

# Sex distribution
asians_rs671_sex <- asians_rs671 %>%
  mutate(
    sex_at_birth = case_when(
      sex_at_birth == "Male" ~ "Male",
      sex_at_birth == "Female" ~ "Female",
      TRUE ~ "Other"
    )
  ) %>%
  group_by(sex_at_birth) %>%
  summarise(
    n_people = n(),
    percent = round(100 * n() / nrow(asians_rs671), 2)
  ) %>%
  arrange(desc(n_people))

#ASIANS ONLY, without rs671
# Filter non-rs671 cohort for Asians
asians_no_rs671 <- withoutrs671 %>%
  filter(race_ethnicity == "Asian")
dim(asians_no_rs671) #10999
# Age distribution
asians_no_rs671_age <- asians_no_rs671 %>%
  group_by(age_group) %>%
  summarise(
    n_people = n(),
    percent = round(100 * n() / nrow(asians_no_rs671), 2)
  ) %>%
  arrange(factor(age_group, 
                 levels = c("18-29", "30-39", "40-49", "50-59",
                            "60-69", "70-79", "80-89", "90+")))

# Sex distribution
asians_no_rs671_sex <- asians_no_rs671 %>%
  mutate(
    sex_at_birth = case_when(
      sex_at_birth == "Male" ~ "Male",
      sex_at_birth == "Female" ~ "Female",
      TRUE ~ "Other"
    )
  ) %>%
  group_by(sex_at_birth) %>%
  summarise(
    n_people = n(),
    percent = round(100 * n() / nrow(asians_no_rs671), 2)
  ) %>%
  arrange(desc(n_people))

#check all totals for asians
total_asians_rs671 <- sum(asians_rs671_age$n_people)
total_asians_rs671
total_asians_rs671_sex <- sum(asians_rs671_sex$n_people)
total_asians_rs671_sex
##^^both tables have 3148 people, checks out
total_asians_no_rs671 <- sum(asians_no_rs671_age$n_people)
total_asians_no_rs671
total_asians_no_rs671_sex <- sum(asians_no_rs671_sex$n_people)
total_asians_no_rs671_sex
##^^both tables have 10999 people, checks out
#add 10999+3148 = 14147 which should equal asians in total dataset
asians_total <- sankeydata_gene %>%
  filter(race_ethnicity == "Asian") %>%
  summarise(total = n())

asians_total #equals 14147, checks out

#Simple race breakdown of with and without rs671:
library(dplyr)

withrs671_counts <- withrs671 %>%
  count(race_ethnicity) %>%
  rename(with_rs671 = n)

withoutrs671_counts <- withoutrs671 %>%
  count(race_ethnicity) %>%
  rename(without_rs671 = n)

race_breakdown <- full_join(
  withrs671_counts,
  withoutrs671_counts,
  by = "race_ethnicity"
)

#how many people are aged 18-21?
library(dplyr)
library(lubridate)

withoutrs671 %>%
  mutate(
    age = as.integer(interval(date_of_birth, Sys.Date()) / years(1))
  ) %>%
  filter(age >= 18, age <= 21) %>%
  summarise(
    n_18_21 = n()
  )
