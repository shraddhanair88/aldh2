# Cross Asian ethnicity information with avg_drink_rs671_asian

# Remove PMI / Skip / empty / "NA" responses
avg_drink_rs671_asian <- avg_drink_rs671_asian %>%
  filter(!answer %in% c("PMI: Prefer Not To Answer", "PMI: Skip", "", "NA"))

# Make person_id the same type
asianethnicity <- asianethnicity %>% mutate(person_id = as.character(person_id))
avg_drink_rs671_asian <- avg_drink_rs671_asian %>% mutate(person_id = as.character(person_id))

# Left join
rs671_asian_avgdrink_ethnicity <- left_join(avg_drink_rs671_asian, asianethnicity, by = "person_id")

# Remove rows with NA ethnicity or drink response
rs671_asian_avgdrink_ethnicity_clean <- rs671_asian_avgdrink_ethnicity %>%
  filter(!is.na(answer.x), !is.na(answer.y))

# Collapse drink levels
rs671_asian_avgdrink_ethnicity_clean <- rs671_asian_avgdrink_ethnicity_clean %>%
  mutate(answer.x = case_when(
    answer.x == "Average Daily Drink Count: 1 or 2" ~ "1–2 drinks",
    answer.x %in% c(
      "Average Daily Drink Count: 3 or 4",
      "Average Daily Drink Count: 5 or 6",
      "Average Daily Drink Count: 7 to 9",
      "Average Daily Drink Count: 10 or More"
    ) ~ "3+ drinks"
  ))

# Set order??
avg_drink_order <- c("1–2 drinks", "3+ drinks")

rs671_asian_avgdrink_ethnicity_clean <- rs671_asian_avgdrink_ethnicity_clean %>%
  mutate(answer.x = factor(answer.x, levels = avg_drink_order))

#make tables
#Chinese
asian_chinese_avgdrink_carrier <- rs671_asian_avgdrink_ethnicity_clean %>%
  filter(answer.y == "Asian Specific: Chinese") %>%
  group_by(answer.x) %>%
  summarise(count = n(), .groups = "drop") %>%
  mutate(percent = 100 * count / sum(count),
         value = paste0(count, " (", sprintf("%.2f", percent), "%)")) %>%
  select(answer = answer.x, `Asian Specific: Chinese` = value)

#Japanese
asian_japanese_avgdrink_carrier <- rs671_asian_avgdrink_ethnicity_clean %>%
  filter(answer.y == "Asian Specific: Japanese") %>%
  group_by(answer.x) %>%
  summarise(count = n(), .groups = "drop") %>%
  mutate(percent = 100 * count / sum(count),
         value = paste0(count, " (", sprintf("%.2f", percent), "%)")) %>%
  select(answer = answer.x, `Asian Specific: Japanese` = value)

#Korean
asian_korean_avgdrink_carrier <- rs671_asian_avgdrink_ethnicity_clean %>%
  filter(answer.y == "Asian Specific: Korean") %>%
  group_by(answer.x) %>%
  summarise(count = n(), .groups = "drop") %>%
  mutate(percent = 100 * count / sum(count),
         value = paste0(count, " (", sprintf("%.2f", percent), "%)")) %>%
  select(answer = answer.x, `Asian Specific: Korean` = value)

#Filipino
asian_filipino_avgdrink_carrier <- rs671_asian_avgdrink_ethnicity_clean %>%
  filter(answer.y == "Asian Specific: Filipino") %>%
  group_by(answer.x) %>%
  summarise(count = n(), .groups = "drop") %>%
  mutate(percent = 100 * count / sum(count),
         value = paste0(count, " (", sprintf("%.2f", percent), "%)")) %>%
  select(answer = answer.x, `Asian Specific: Filipino` = value)

#Vietnamese
asian_vietnamese_avgdrink_carrier <- rs671_asian_avgdrink_ethnicity_clean %>%
  filter(answer.y == "Asian Specific: Vietnamese") %>%
  group_by(answer.x) %>%
  summarise(count = n(), .groups = "drop") %>%
  mutate(percent = 100 * count / sum(count),
         value = paste0(count, " (", sprintf("%.2f", percent), "%)")) %>%
  select(answer = answer.x, `Asian Specific: Vietnamese` = value)

##NO RS671
# Remove PMI / Skip / empty / "NA" responses
avg_drink_nors671_asian <- avg_drink_nors671_asian %>%
  filter(!answer %in% c("PMI: Prefer Not To Answer", "PMI: Skip", "", "NA"))

# Make person_id the same type
asianethnicity <- asianethnicity %>% mutate(person_id = as.character(person_id))
avg_drink_nors671_asian <- avg_drink_nors671_asian %>% mutate(person_id = as.character(person_id))

# Left join ethnicity
nors671_asian_avgdrink_ethnicity <- left_join(
  avg_drink_nors671_asian,
  asianethnicity,
  by = "person_id"
)

# Remove rows with NA
nors671_asian_avgdrink_ethnicity_clean <- nors671_asian_avgdrink_ethnicity %>%
  filter(!is.na(answer.x), !is.na(answer.y))

nors671_asian_avgdrink_ethnicity_clean <- nors671_asian_avgdrink_ethnicity_clean %>%
  mutate(answer.x = case_when(
    answer.x == "Average Daily Drink Count: 1 or 2" ~ "1–2 drinks",
    answer.x %in% c(
      "Average Daily Drink Count: 3 or 4",
      "Average Daily Drink Count: 5 or 6",
      "Average Daily Drink Count: 7 to 9",
      "Average Daily Drink Count: 10 or More"
    ) ~ "3+ drinks"
  ))

avg_drink_order <- c("1–2 drinks", "3+ drinks")

nors671_asian_avgdrink_ethnicity_clean <- nors671_asian_avgdrink_ethnicity_clean %>%
  mutate(answer.x = factor(answer.x, levels = avg_drink_order))

##NO RS671 tables

#Chinese
asian_chinese_avgdrink_noncarrier <- nors671_asian_avgdrink_ethnicity_clean %>%
  filter(answer.y == "Asian Specific: Chinese") %>%
  group_by(answer.x) %>%
  summarise(count = n(), .groups = "drop") %>%
  mutate(percent = 100 * count / sum(count),
         value = paste0(count, " (", sprintf("%.2f", percent), "%)")) %>%
  select(answer = answer.x, `Asian Specific: Chinese` = value)

#Japanese
asian_japanese_avgdrink_noncarrier <- nors671_asian_avgdrink_ethnicity_clean %>%
  filter(answer.y == "Asian Specific: Japanese") %>%
  group_by(answer.x) %>%
  summarise(count = n(), .groups = "drop") %>%
  mutate(percent = 100 * count / sum(count),
         value = paste0(count, " (", sprintf("%.2f", percent), "%)")) %>%
  select(answer = answer.x, `Asian Specific: Japanese` = value)

#Korean
asian_korean_avgdrink_noncarrier <- nors671_asian_avgdrink_ethnicity_clean %>%
  filter(answer.y == "Asian Specific: Korean") %>%
  group_by(answer.x) %>%
  summarise(count = n(), .groups = "drop") %>%
  mutate(percent = 100 * count / sum(count),
         value = paste0(count, " (", sprintf("%.2f", percent), "%)")) %>%
  select(answer = answer.x, `Asian Specific: Korean` = value)

#Filipino
asian_filipino_avgdrink_noncarrier <- nors671_asian_avgdrink_ethnicity_clean %>%
  filter(answer.y == "Asian Specific: Filipino") %>%
  group_by(answer.x) %>%
  summarise(count = n(), .groups = "drop") %>%
  mutate(percent = 100 * count / sum(count),
         value = paste0(count, " (", sprintf("%.2f", percent), "%)")) %>%
  select(answer = answer.x, `Asian Specific: Filipino` = value)

#Vietnamese
asian_vietnamese_avgdrink_noncarrier <- nors671_asian_avgdrink_ethnicity_clean %>%
  filter(answer.y == "Asian Specific: Vietnamese") %>%
  group_by(answer.x) %>%
  summarise(count = n(), .groups = "drop") %>%
  mutate(percent = 100 * count / sum(count),
         value = paste0(count, " (", sprintf("%.2f", percent), "%)")) %>%
  select(answer = answer.x, `Asian Specific: Vietnamese` = value)