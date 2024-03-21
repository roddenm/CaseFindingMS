#RALF Present Flag (Does your project contain RALF codes? Set TRUE to obtain RALF/LSOA info)
RALF_PRESENT <- TRUE

#RData Folders for Dataframes
PREPROC_DIR <- "DataFrames (Pre Processing)/"
COHORT_DIR <- "DataFrames (Cohorts)/"
RAWSQL_DIR <- "DataFrames (Raw SQL)/"
INCPREV_DIR <- "DataFrames (Incidence Prevalence)/"
#Output Folders
OUT_DIR <- "Output/"
TESTOUT_DIR <- "Output (Tests)/"
OUT_DIR_ALIVE <- "Output (Alive)/"
TESTOUT_DIR_ALIVE <- "Output (Tests Alive)/"
EVENTOUT_DIR <- "Time To Event Output/"

#Make the folders if they don't exist
if(!dir.exists(paste(WORKING_DIR,OUT_DIR_ALIVE,sep="")))
{
  dir.create(paste(WORKING_DIR,OUT_DIR_ALIVE,sep=""))
}

if(!dir.exists(paste(WORKING_DIR,TESTOUT_DIR_ALIVE,sep="")))
{
  dir.create(paste(WORKING_DIR,TESTOUT_DIR_ALIVE,sep=""))
}

if(!dir.exists(paste(WORKING_DIR,PREPROC_DIR,sep="")))
{
  dir.create(paste(WORKING_DIR,PREPROC_DIR,sep=""))
}

if(!dir.exists(paste(WORKING_DIR,COHORT_DIR,sep="")))
{
  dir.create(paste(WORKING_DIR,COHORT_DIR,sep=""))
}

if(!dir.exists(paste(WORKING_DIR,RAWSQL_DIR,sep="")))
{
  dir.create(paste(WORKING_DIR,RAWSQL_DIR,sep=""))
}

if(!dir.exists(paste(WORKING_DIR,INCPREV_DIR,sep="")))
{
  dir.create(paste(WORKING_DIR,INCPREV_DIR,sep=""))
}

if(!dir.exists(paste(WORKING_DIR,OUT_DIR,sep="")))
{
  dir.create(paste(WORKING_DIR,OUT_DIR,sep=""))
}

if(!dir.exists(paste(WORKING_DIR,EVENTOUT_DIR,sep="")))
{
  dir.create(paste(WORKING_DIR,EVENTOUT_DIR,sep=""))
}

#Enter project number here
PROJECT_NUMBER <- "SAIL0945V"
PROJECT_WORK_NUMBER <- "SAILW0945V"

YEAR_MIN <- 1970
YEAR_MAX <- 2020

DATE_MIN <- paste(YEAR_MIN,"-01-01",sep="")
DATE_MAX <- paste(YEAR_MAX,"-12-31",sep="")