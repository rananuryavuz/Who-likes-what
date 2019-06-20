library(jsonlite)
library(data.table)
library(tidyverse)

root						<- "C:/Users/The Soviet Unit/Documents/R/Who Likes What/"
# root						<- "D:/Users/afattore/Documents/R/data science/Who Likes What/"
impDir          <- paste0(root,"imports", collapse = "")
dataDir         <- paste0(root,"data", collapse = "")
outDir          <- paste0(root,"outputs", collapse = "")
utDir           <- paste0(root,"utilities",	collapse = "")

rm("root")

if(exists("dset")) {dset <- NULL}

# create the list of likes from raw data. The expected encoding is blocks of four elements, as follow:
#   1 - contains the post id
#   2 - post creation time
#   3 - the list of likes coming from the facebook api
#   4 - page information inputed by the user
# the resulting dataset contains a row for each like

setwd(impDir)

file_tags <- data.table(tag = seq(1,747))
files <- paste0("facebook_download_",file_tags[,tag])
files <- paste0(files,".json")
temp_tables <- paste0("chunk_",file_tags[,tag])

for(file in files){
  print(paste0("processing ", file))
  idx <- which(files == file)
  assign(temp_tables[idx], data.table(read_json(file, simplifyVector = T)))
}
dset <- rbindlist(mget(temp_tables), fill = T)
rm(list = temp_tables)

rm("files","file", "file_tags", "idx", "temp_tables")

# save the file for future use

setwd(dataDir); fwrite(dset, "armani_dataset.csv")

# ingest user visual information:
#   1 - set the correct directory
#   2 - set tge counter to the number of files
#   3 - read the files into two separate tables (user picture and cover photo)
#   4 - merge them by user id
#   5 - save the file for future use

setwd(impDir)

tags                <- seq(1,6489)
temp_picture_tables <- paste0("picture_",tags)
temp_photo_tables   <- paste0("photo_",tags)

for(tag in tags){
  print(paste0("processing file ", tag))
  picture_file  <- paste0(paste0("user_picture_",tag),".json")
  photo_file    <- paste0(paste0("cover_photo_",tag),".json")
  assign(temp_picture_tables[tag], data.table(read_json(picture_file, simplifyVector = T)))
  assign(temp_photo_tables[tag], data.table(read_json(photo_file, simplifyVector = T)))
}

user_pictures_set <- rbindlist(mget(temp_picture_tables), fill = T)
cover_photos_set  <- rbindlist(mget(temp_photo_tables), fill = T)

image_set <- merge(user_pictures_set, cover_photos_set, by = "user_id")
image_set <- image_set[picture != "none retrieved"]

rm(list = c(temp_picture_tables, temp_photo_tables, "user_pictures_set", "cover_photos_set", "file_tags", "files", "photo_file", "picture_file", "tag", "tags"))
rm("temp_photo_tables","temp_picture_tables")

setwd(dataDir); fwrite(image_set, "armani_image_set.csv")
