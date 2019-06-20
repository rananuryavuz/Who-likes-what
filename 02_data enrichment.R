library(data.table)
library(tidyverse)
library(broom)
library(lubridate)
library(jsonlite)
library(plyr)

root						<- "C:/Users/The Soviet Unit/Documents/R/Who Likes What/"
# root						<- "D:/Users/afattore/Documents/R/data science/Who Likes What/"
dataDir         <- paste0(root,"data")
outDir          <- paste0(root,"outputs")
utDir           <- paste0(root,"utilities")
expDir          <- paste0(root,"exports")
impDir          <- paste0(root,"imports")
prpDir          <- paste0(root,"data/pre_processed")

rm("root")

setwd(prpDir)

dset <- read_csv(
  "armani_dataset.csv",
  col_types = cols(
    id = col_character(),
    total_likes = col_number(),
    total_comments = col_number(),
    total_shares = col_number()
  )
) %>% data.table()

# rename columns and preprocess data types where needed, then add a column
# with the first names of users. This will be used to assign gender where
# picture recognition fails.

dset[,date := as.Date(created_time)]
dset[is.na(total_shares), total_shares := 0]
dset[is.na(picture), picture := "none"]
dset[is.na(message), message := "none"]
dset[, year := year(created_time)]
dset[, month := month(created_time)]
dset[, day_of_week := wday(created_time, label = T)]
dset[, first_name := as.character(lapply(strsplit(as.character(name), " "),"[",1))]

# fwrite(dset, "armani_working_dataset.csv")

# create and export datasets for reference outside R

page_list <- dset[,.(.N), by = .(page)]
user_list <- dset[,.(.N), by = .(id, name)]
post_list <- dset[,.(.N), by = .(post)]
post_pictures <- dset[,.(.N), by = .(post,picture)]
year_list <- dset[,.(.N), by = .(year)]

fwrite(page_list, "pages.csv")
fwrite(user_list, "users.csv")
fwrite(post_pictures, "pictures.csv")

# prepare the export files for picture analysis with clarifai. These are three files:
#   1 - pictures from the posts, with the post ID and picture URL
#   2 - user profile pictures, with user ID's
#   3 - user cover photos, with user ID's

post_pictures <- dset[,.(.N), by = .(post, picture)][,.(post, url = picture)]
user_pictures <- dset[user_picture != "none retrieved", .(.N), by = .(id, user_picture)][,.(id, url = user_picture)]
cover_pictures <- dset[cover != "none retrieved", .(.N), by = .(id, cover)][,.(id, url = cover)]

setwd(expDir)
fwrite(post_pictures, "post_pictures.csv")
fwrite(user_pictures, "user_pictures.csv")
fwrite(cover_pictures, "cover_pictures.csv")

# now the analysis is handed over to clarifai. The expectation is that the three image lists will be
# analyzed in turn with the following models:
#   post_pictures:  "general concepts"
#   user_pictures:  "demographics", "general concepts"
#   cover_pictures: "general concepts"
# this analyses are expected to return lists of .json files, with the prefixes
# "picture_analysis_by_" and a suffix indicating the type of model used, such as "general_", plus
# a file ordinal number, and the ".json" extension.
# once these are returned, the usual upload and merging procedure is followed, to complete the dataset.
# Note that when using the clarifai import app, the different sets of analyses MUST be imported right after
# being run, or each overwrites the one before. Therefore, when using the below section to import new data,
# make sure you are aware of the dataset you're referring to.

setwd(impDir)
tags = seq(1,1242)
tempvars = paste0("chunk_",tags)
for(tag in tags){
  print(paste0("processing file # ",tag))
  file = paste0(paste0("picture_analysis_by_demographics_",tag),".json")
  assign(tempvars[tag],read_json(file, simplifyVector = T)) %>% data.table()
}

picture_set <- rbindlist(mget(tempvars), fill = T)
rm(list = c(tempvars, "tag", "tags", "file"))
rm(tempvars)

setwd(prpDir); fwrite(picture_set,"armani_user_pictures_demographics.csv")

rm(picture_set)

# once all the data are collected and properly stored, they can be merged into the dataset
# the relevant files are assumed to be saved in the preprocessing directory
# they are loaded in turn and merged into the main dataset by the relevant key.
# mind the column names across the files, make sure they're recognizable when merged

setwd(prpDir)
up_gm <- read_csv("armani_user_pictures_general_model.csv", col_types = cols(id = col_character())) %>% data.table()
pp_gm <- read_csv("armani_post_pictures_general_model.csv", col_types = cols(post = col_character())) %>% data.table()
cp_gm <- read_csv("armani_cover_pictures_general_model.csv", col_types = cols(id = col_character())) %>% data.table()
up_dm <- read_csv("armani_user_pictures_demographics.csv", col_types = cols(id = col_character())) %>% data.table()

pp_gm[,duplicates := duplicated(post)]
pp_gm <- pp_gm[duplicates == F][,duplicates := NULL]

# demographics need further simplification before merging, in particular on behalf of the age group
# age is estimated by clarifai as a distribution of probabilities over each year of age, from 0 to 99
# with a cutoff to zero for ages estimated too high or too low. The age is estimated within a named
# age group, based on the sum of probabilities within each age group - the group which sums to the
# highest probability is selected. To do this, the raw demographic file is 'melt' in a table with three
# columns: user id, age group, and probability, with one observation per user per age group.
# To assign age groups, each age is assigned a numeric value, then split in age breaks, and labeled (using cut()).
# Finally, the table is summarized by sum of probability per age group, and the result is further summarized
# by taking only the age group with the highest sum of probabilities, using top_n().

measure_vars <- paste0("demo_age_",seq(1,99))
id_vars <- c("demo_gender","id","url")
up_dm <- melt(up_dm, id.vars = id_vars, measure.vars = measure_vars, variable.name = "age", value.name = "probability")
up_dm[, age := factor(age)]
up_dm[, age := mapvalues(age, from = levels(age), to = seq(1,99))]
up_dm[, age := as.numeric(age)]
age_labels <- c("infant","kid","teen","tween","young_adult","central_group","mature","old")
age_breaks <- c(0,5,12,19,29,39,55,65,99)
up_dm[, age_group := cut(age, age_breaks, labels = age_labels, include.lowest = T)]
up_dm <- dcast(up_dm, id + demo_gender ~ age_group, value.var = "probability", fun.aggregate = sum)
up_dm <- up_dm %>% 
  melt(id.vars = c("id","demo_gender"), measure.vars = age_labels, variable.name = "demo_age_group", value.name = "weight") %>%
  group_by(id) %>%
  top_n(1,wt = weight) %>%
  ungroup() %>%
  as.data.table()

up_dm_missing <- unique(up_dm[weight == 0][, demo_age_group := "none recognizable"])
up_dm <- up_dm[weight != 0]
up_dm <- rbind(up_dm, up_dm_missing)

dset <- merge(dset, up_gm[,.(id,user_picture_concepts = concepts)], by = "id", all.x = T)
dset <- merge(dset, cp_gm[,.(id, cover_picture_concepts = concepts)], by = "id", all.x = T)
dset <- merge(dset, up_dm[,.(id, demo_gender, demo_age_group)], by = "id", all.x = T)
dset <- merge(dset, pp_gm[,.(post, post_picture_concepts = concepts)], by = "post", all.x = T)

rm("up_gm","pp_gm","cp_gm","up_dm","up_dm_missing","age_breaks","age_labels","id_vars","measure_vars")

# we try and glean gender information from names: first we create a column in the dataset
# to store the first names of each user, then we create a table of just the first names,
# and (to facilitate classification, which will have to be done manually), we check
# whether the names end in 'o' (likely male), 'a', 'e' or 'i'. Then the file is saved
# and inspected off-line, to assign gender. Mind that re-running this procedure
# once the list is prepared, will overwrite the existing list, including the
# classification already done. Backup the file.

names <- dset[,.(.N), by = .(first_name)][order(-N)]
name_list[,ends_in_o := grepl("[[:alpha:]]o\\b",first_name)]
name_list[,ends_in_a := grepl("[[:alpha:]]a\\b",first_name)]
name_list[,ends_in_e := grepl("[[:alpha:]]e\\b",first_name)]
name_list[,ends_in_i := grepl("[[:alpha:]]i\\b",first_name)]
setwd(dataDir); fwrite(name_list, "name_list.csv")

# if the name list has already been created, load it and use to assign gender
# where it is not yet set: filter the dataset accordingly, then assign the gender
# to male and female names respectively

setwd(dataDir); name_list <- fread("name_list.csv")
male_names <- as.vector(name_list[demo_gender == "male", first_name])
female_names <- as.vector(name_list[demo_gender == "female", first_name])
dset[(demo_gender == "unclear" | demo_gender == "none_recognizable") & first_name %in% male_names, demo_gender := "male"]
dset[(demo_gender == "unclear" | demo_gender == "none_recognizable") & first_name %in% female_names, demo_gender := "female"]

rm("name_list","female_names","male_names")

# to add gender information where it was not possible through pure facial or name recognition
# we first attemtp to classify based on the (manually compiled) list of names above. Where
# this fails, we supplement by analyzing the concepts from the user picture. We repeat the procedure
# for males and females. First, we add to the table a column where we record whether
# in the concepts both the terms 'one' and 'man' are present, then forecast as male
# those users. Then we repeat for females, and run again the same analyis based
# on the cover photo

gender_unclear <- dset[(demo_gender == "unclear" | demo_gender == "none_recognizable") & !is.na(user_picture_concepts), .(.N), by = .(id, name, demo_gender, user_picture_concepts, cover_picture_concepts)]
gender_unclear[, man := grepl("\\bman\\b", user_picture_concepts)]
gender_unclear[, woman := grepl("woman", user_picture_concepts)]
gender_unclear[, one := grepl("one", user_picture_concepts)]
gender_unclear[man & !woman & one, demo_gender := "male"]
gender_unclear[!man & woman & one, demo_gender := "female"]

gender_unclear[, man2 := grepl("\\bman\\b", cover_picture_concepts)]
gender_unclear[, woman2 := grepl("woman", cover_picture_concepts)]
gender_unclear[, one2 := grepl("one", cover_picture_concepts)]
gender_unclear[(demo_gender == "unclear" | demo_gender == "none_recognizable") & man2 & !woman2 & one2, demo_gender := "male"]
gender_unclear[(demo_gender == "unclear" | demo_gender == "none_recognizable") & !man2 & woman2 & one2, demo_gender := "female"]

male_ids <- gender_unclear[demo_gender == "male", .(id)]
female_ids <- gender_unclear[demo_gender == "female", .(id)]

dset[id %in% male_ids[,id], demo_gender := "male"]
dset[id %in% female_ids[,id], demo_gender := "female"]

gender_age <- dset[!is.na(demo_gender),.(.N), by = .(id, demo_gender, demo_age_group)]
ggplot(gender_age, aes(demo_gender, fill = demo_age_group)) + geom_bar()

rm("female_ids","gender_age","gender_unclear","male_ids")

setwd(dataDir); fwrite(dset, "armani_working_dataset.csv")
