ALL_FLAG <- FALSE

if(ALL_FLAG==TRUE)
{
  load(paste(WORKING_DIR,COHORT_DIR,"Clean Population Table (V1).RData",sep=""))
  load(paste(WORKING_DIR,COHORT_DIR,"MS Cohort (V2 - UKMSR).RData",sep=""))
  load(paste(WORKING_DIR,COHORT_DIR,"Demye Only Users.RData",sep=""))
}else
{
  load(paste(WORKING_DIR,COHORT_DIR,"Clean Population Table (V1 Alive).RData",sep=""))
  load(paste(WORKING_DIR,COHORT_DIR,"MS Cohort (V2 - UKMSR) Alive.RData",sep=""))
  load(paste(WORKING_DIR,COHORT_DIR,"Demye Only Users Alive.RData",sep=""))
  
  cleanPopulationTable <- cleanPopulationTable_Alive
  rm(cleanPopulationTable_Alive)
  
  msCohort_UKMSR <- msCohortAlive_UKMSR
  
  demyeOnlyUsers <- demyeOnlyUsers_Alive
  
  OUT_DIR <- OUT_DIR_ALIVE
  TESTOUT_DIR <- TESTOUT_DIR_ALIVE
}