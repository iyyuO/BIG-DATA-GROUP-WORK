library(data.table)
library(stringr)
library(parallel)

#Read CSV
data <- fread("job_descriptions.csv", select = c("Role", "skills"))

#Count missing/blank values
na_skills <- sum(is.na(data$skills))
blank_skills <- sum(data$skills == "", na.rm = TRUE)
print(paste("Missing 'Skills':", na_skills))
print(paste("Blank 'Skills':", blank_skills))

#Split data into chunks based on available cores
n_cores <- detectCores() - 1
split_chunks <- split(data, sort(rep(1:n_cores, length.out = nrow(data))))

#Define the map function
map_function <- function(chunk) {
  chunk <- na.omit(chunk)
  chunk[, skills := tolower(skills)]
  chunk[, skills := str_split(skills, ",\\s*")]
  
  exploded <- chunk[, .(skill = unlist(skills)), by = Role]
  exploded[, .N, by = .(Role, skill)]
}

#Start cluster
cl <- makeCluster(n_cores)

#Export required objects and libraries
clusterExport(cl, varlist = c("map_function"), envir = environment())
clusterEvalQ(cl, {
  library(data.table)
  library(stringr)
})

#Run map step in parallel
mapped_results <- parLapply(cl, split_chunks, map_function)

#Stop cluster
stopCluster(cl)

#Reduce step: combine and aggregate results
all_counts <- rbindlist(mapped_results)
final_counts <- all_counts[, .(Total = sum(N)), by = .(Role, skill)][order(Role, -Total)]

#Get top 5 skills per role
top_skills_by_role <- final_counts[, head(.SD, 5), by = Role]

#Output result
print(top_skills_by_role)
