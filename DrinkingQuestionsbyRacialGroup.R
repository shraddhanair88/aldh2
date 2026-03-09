#Frequency and Quantity Questions by Racial Group
library(dplyr)
library(tidyr)
#DRINKING FREQUENCY

# Make person_id the same type
finalcohort <- finalcohort %>% mutate(person_id = as.character(person_id))
drink_frequency_past_year <- drink_frequency_past_year %>% mutate(person_id = as.character(person_id))

#clean PMI and Skip responses!
drink_frequency_past_year <- drink_frequency_past_year %>%
  filter(!answer %in% c("PMI: Prefer Not To Answer", "PMI: Skip", "", "NA"))

unique(drink_frequency_past_year$answer)

# Cross/join with finalcohort
finalcohort_drink_freq <- left_join(
  finalcohort, 
  drink_frequency_past_year, 
  by = "person_id"
)
table(finalcohort_drink_freq$answer)

#delete rows that didn't have an answer
finalcohort_drink_freq_clean <- finalcohort_drink_freq %>%
  filter(!is.na(answer), answer != "", answer != "NA")

nrow(finalcohort_drink_freq) #447821
nrow(finalcohort_drink_freq_clean) #385193 ~54583 rows deleted

table(finalcohort_drink_freq_clean$race_ethnicity)

# Define the order for drinking frequency
drink_frequency_order <- c(
  "Drink Frequency Past Year: Never",
  "Drink Frequency Past Year: Monthly Or Less",
  "Drink Frequency Past Year: 2 to 4 Per Month",
  "Drink Frequency Past Year: 2 to 3 Per Week",
  "Drink Frequency Past Year: 4 or More Per Week"
)

unique(finalcohort_drink_freq_clean$race_ethnicity)
table(finalcohort_drink_freq_clean$race_ethnicity)

# White
white_drink_freq <- finalcohort_drink_freq_clean %>%
  filter(race_ethnicity == "White") %>%
  mutate(answer = factor(answer, levels = drink_frequency_order)) %>%
  group_by(answer) %>%
  summarise(count = n(), .groups = "drop") %>%
  mutate(percent = 100 * count / sum(count),
         value = paste0(count, " (", sprintf("%.2f", percent), "%)")) %>%
  select(answer, value) %>%
  arrange(answer)

# Asian
asian_drink_freq <- finalcohort_drink_freq_clean %>%
  filter(race_ethnicity == "Asian") %>%
  mutate(answer = factor(answer, levels = drink_frequency_order)) %>%
  group_by(answer) %>%
  summarise(count = n(), .groups = "drop") %>%
  mutate(percent = 100 * count / sum(count),
         value = paste0(count, " (", sprintf("%.2f", percent), "%)")) %>%
  select(answer, value) %>%
  arrange(answer)

# Black
black_drink_freq <- finalcohort_drink_freq_clean %>%
  filter(race_ethnicity == "Black or African American") %>%
  mutate(answer = factor(answer, levels = drink_frequency_order)) %>%
  group_by(answer) %>%
  summarise(count = n(), .groups = "drop") %>%
  mutate(percent = 100 * count / sum(count),
         value = paste0(count, " (", sprintf("%.2f", percent), "%)")) %>%
  select(answer, value) %>%
  arrange(answer)

# Hispanic
hispanic_drink_freq <- finalcohort_drink_freq_clean %>%
  filter(race_ethnicity == "Hispanic or Latino") %>%
  mutate(answer = factor(answer, levels = drink_frequency_order)) %>%
  group_by(answer) %>%
  summarise(count = n(), .groups = "drop") %>%
  mutate(percent = 100 * count / sum(count),
         value = paste0(count, " (", sprintf("%.2f", percent), "%)")) %>%
  select(answer, value) %>%
  arrange(answer)

#DRINKING QUANTITY
# Make person_id the same type
finalcohort <- finalcohort %>% mutate(person_id = as.character(person_id))
average_daily_drink_count <- average_daily_drink_count %>% mutate(person_id = as.character(person_id))

# Clean PMI and Skip responses
average_daily_drink_count <- average_daily_drink_count %>%
  filter(!answer %in% c("PMI: Prefer Not To Answer", "PMI: Skip", "", "NA"))

# Cross/join with finalcohort
finalcohort_avg_drink <- left_join(
  finalcohort,
  average_daily_drink_count,
  by = "person_id"
)

# Delete rows without an answer
finalcohort_avg_drink_clean <- finalcohort_avg_drink %>%
  filter(!is.na(answer), answer != "", answer != "NA")

# Define the order for drinking quantity
avg_drink_order <- c(
  "Average Daily Drink Count: 1 or 2",
  "Average Daily Drink Count: 3 or 4",
  "Average Daily Drink Count: 5 or 6",
  "Average Daily Drink Count: 7 to 9",
  "Average Daily Drink Count: 10 or More"
)

# White
white_avg_drink <- finalcohort_avg_drink_clean %>%
  filter(race_ethnicity == "White") %>%
  mutate(answer = factor(answer, levels = avg_drink_order)) %>%
  group_by(answer) %>%
  summarise(count = n(), .groups = "drop") %>%
  mutate(percent = 100 * count / sum(count),
         value = paste0(count, " (", sprintf("%.2f", percent), "%)")) %>%
  select(answer, value) %>%
  arrange(answer)

# Asian
asian_avg_drink <- finalcohort_avg_drink_clean %>%
  filter(race_ethnicity == "Asian") %>%
  mutate(answer = factor(answer, levels = avg_drink_order)) %>%
  group_by(answer) %>%
  summarise(count = n(), .groups = "drop") %>%
  mutate(percent = 100 * count / sum(count),
         value = paste0(count, " (", sprintf("%.2f", percent), "%)")) %>%
  select(answer, value) %>%
  arrange(answer)

# Black
black_avg_drink <- finalcohort_avg_drink_clean %>%
  filter(race_ethnicity == "Black or African American") %>%
  mutate(answer = factor(answer, levels = avg_drink_order)) %>%
  group_by(answer) %>%
  summarise(count = n(), .groups = "drop") %>%
  mutate(percent = 100 * count / sum(count),
         value = paste0(count, " (", sprintf("%.2f", percent), "%)")) %>%
  select(answer, value) %>%
  arrange(answer)

# Hispanic
hispanic_avg_drink <- finalcohort_avg_drink_clean %>%
  filter(race_ethnicity == "Hispanic or Latino") %>%
  mutate(answer = factor(answer, levels = avg_drink_order)) %>%
  group_by(answer) %>%
  summarise(count = n(), .groups = "drop") %>%
  mutate(percent = 100 * count / sum(count),
         value = paste0(count, " (", sprintf("%.2f", percent), "%)")) %>%
  select(answer, value) %>%
  arrange(answer)