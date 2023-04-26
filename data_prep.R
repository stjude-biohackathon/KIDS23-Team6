library(anytime)

readfile <- read.csv("/research_jude/rgs01_jude/groups/geelegrp/projects/2023_biohackathon/common/lsf.csv")

new_df <- data.frame(startTime = anytime(readfile$startTime),
                     submitTime = anytime(readfile$submitTime),
                     pendTime = readfile$startTime - readfile$submitTime,
                     numProcs = readfile$numProcessors,
                     queue = readfile$queue)

cpus_per <- data.frame(second = c (1682089142:1682116460))

cores_reserved <- vector(, 27319)
#colnames(cpus_per) <- c("second", "cores_reserved")

cpus_per = cbind(cpus_per, cores_reserved)

for (i in 1:nrow(readfile)){
  `%+=%` = function(e1,e2) eval.parent(substitute(e1 <- e1 + e2))
  cpus_per$cores_reserved[cpus_per$second <= readfile[i, "Event.Time"] & cpus_per$second >= readfile[i, "startTime"]] %+=% 1
}

table(cpus_per$cores_reserved)
head(new_df)
head(cpus_per)