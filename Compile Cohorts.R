library(tidyverse)
library(lubridate)
library(compareGroups)

#Main Project Working Directory
WORKING_DIR <- "S:/0945 - UK MS Register/msincidence (JNNP Final)/"
source(paste(WORKING_DIR,"Define File Constants.R",sep=""))

load(paste(WORKING_DIR,"Patients Master Frame.RData",sep=""))
load(paste(WORKING_DIR,"MS Event Counts.RData",sep=""))
load(paste(WORKING_DIR,RAWSQL_DIR,"Portal Demographics (Raw).RData",sep=""))
load(paste(WORKING_DIR,RAWSQL_DIR,"WDSD People In Wales.RData",sep=""))

#Years of Interest for the study
yearsOfInterest <- as.numeric(YEAR_MIN:YEAR_MAX)

totalPopulationTable <- allPatients_MasterFrame %>%
  select(ALF_PE,Gender,WOB,YearOfBirth,Dead,DeathDate,DeathSource,YearOfDeath,
         #LSOA/RALF Codes (For determining Location)
         LSOA2011_CD,LHB22NM,WDSD_START,WDSD_END,
         LSOA2011_RALF,LHB22NM_RALF,RALF_PE,WDSD_START_RALF,WDSD_END_RALF,
         #Generic MS Columns (So F20.. READ/G35 ICD10)
         MSHospitalOnsetDate,MSHospitalOnsetAge,
         MSGPEventCount,MSHospitalEventCount,
         #This is Generic MS+Extra Codes from Manitoba Paper
         PotentialMSOnsetDate,PotentialMSOnsetAge,
         #When did a patient hit their third/seventh event
         MSThirdEvent,
         MSSeventhEvent,
         #Family History of MS
         FHMSOnsetDate,
         #NMO
         NMOOnsetDate,NMOOnsetAge,
         NMOLatestDate,
         MIN_EVENT_DT,ExitDate,AgeCurrent) %>%
  rename(MinDate=MIN_EVENT_DT) %>%
  mutate(MSHospitalOnsetYear=year(MSHospitalOnsetDate),
         PotentialMSOnsetYear=year(PotentialMSOnsetDate),
         MSEventCount=MSGPEventCount+MSHospitalEventCount) %>%
  mutate(DeathDate=if_else(DeathDate>DATE_MAX,as.Date(NA),DeathDate)) %>%
  mutate(YearOfDeath=if_else(is.na(DeathDate),as.numeric(NA),YearOfDeath)) %>%
  #Calculate the age at death
  mutate(AgeDeath=as.numeric(round((DeathDate-WOB)/365.25,digits=2))) %>%
  #Calculate the age at the end of the study. This is only populated if the patient is still alive
  #at the end study date (Currently 31st December 2020)
  mutate(AgeEnd=if_else(is.na(DeathDate),as.numeric(round((as.Date(DATE_MAX)-WOB)/365.25,digits=2)),NA_real_))

rm(allPatients_MasterFrame)

#Remove those from the populace born before 1910 with no date of death
totalPopulationTable <- totalPopulationTable %>%
  mutate(DeleteFlag=if_else(YearOfBirth<1910&is.na(DeathDate),1,0))

cleanPopulationTable <- totalPopulationTable %>%
  filter(DeleteFlag==0)

rm(totalPopulationTable)

cleanPopulationTable <- cleanPopulationTable %>%
  mutate(UKMSRPatient=if_else(!is.na(MSHospitalOnsetDate)&((PotentialMSOnsetDate-MinDate>=180)|(MSEventCount>=3)),1,0,missing=0)) %>%
  filter(Gender!="Undetermined")

save(cleanPopulationTable,file=paste(WORKING_DIR,COHORT_DIR,"Clean Population Table (V1).RData",sep=""))

cleanPopulationTable_Alive <- cleanPopulationTable %>%
  filter(ALF_PE %in% stillInWales$ALF_PE&Dead==0)
  
save(cleanPopulationTable_Alive,file=paste(WORKING_DIR,COHORT_DIR,"Clean Population Table (V1 Alive).RData",sep=""))

portalDemo <- portalDemo %>%
  left_join(cleanPopulationTable %>% 
               select(ALF_PE,WOB) %>% 
               rename(SAILWOB=WOB),by="ALF_PE") %>% 
  rename(PortalWOB=WOB) %>%
  mutate(WOB=if_else(!is.na(SAILWOB)&SAILWOB!=PortalWOB,SAILWOB,PortalWOB))

save(portalDemo,file=paste(WORKING_DIR,COHORT_DIR,"Portal Demographics (Reformatted).RData",sep=""))

gc()

generateDiagAgeBrackets <- function(frame)
{
  frameWithBrackets <- frame %>%
    mutate(CohortDiagAgeBracket=case_when(CohortDiagAge<10 ~ "Under 10",
                                          CohortDiagAge>=10&CohortDiagAge<15 ~ "10 - 14",
                                          CohortDiagAge>=15&CohortDiagAge<20 ~ "15 - 19",
                                          CohortDiagAge>=20&CohortDiagAge<25 ~ "20 - 24",
                                          CohortDiagAge>=25&CohortDiagAge<30 ~ "25 - 29",
                                          CohortDiagAge>=30&CohortDiagAge<35 ~ "30 - 34",
                                          CohortDiagAge>=35&CohortDiagAge<40 ~ "35 - 39",
                                          CohortDiagAge>=40&CohortDiagAge<45 ~ "40 - 44",
                                          CohortDiagAge>=45&CohortDiagAge<50 ~ "45 - 49",
                                          CohortDiagAge>=50&CohortDiagAge<55 ~ "50 - 54",
                                          CohortDiagAge>=55&CohortDiagAge<60 ~ "55 - 59",
                                          CohortDiagAge>=60&CohortDiagAge<65 ~ "60 - 64",
                                          CohortDiagAge>=65&CohortDiagAge<70 ~ "65 - 69",
                                          CohortDiagAge>=70&CohortDiagAge<75 ~ "70 - 74",
                                          CohortDiagAge>=75&CohortDiagAge<80 ~ "75 - 79",
                                          CohortDiagAge>=80&CohortDiagAge<85 ~ "80 - 84",
                                          CohortDiagAge>=85 ~ "Over 85",
                                          TRUE ~ "Error"))
  
  return(frameWithBrackets)
}

#Get the cohorts we're interested in
#MS
msCohort_UKMSR <- cleanPopulationTable %>%
  filter(UKMSRPatient==1) %>%
  mutate(CohortHow=case_when(PotentialMSOnsetDate-MinDate>=180 ~ "Over 6 Months From Earliest GP Entry",
                             MSEventCount>=3 ~ "3 or More Events")) %>%
  mutate(TimeToTrigger=if_else(CohortHow=="Over 6 Months From Earliest GP Entry",as.numeric(NA),as.numeric(round((MSThirdEvent-PotentialMSOnsetDate)/365.25,digits=2)))) %>%
  rename(CohortDiagYear=PotentialMSOnsetYear) %>%
  rename(CohortDiagAge=PotentialMSOnsetAge)

msCohort_UKMSR <- generateDiagAgeBrackets(msCohort_UKMSR)

save(msCohort_UKMSR,file=paste(WORKING_DIR,COHORT_DIR,"MS Cohort (V2 - UKMSR).RData",sep=""))

msCohortAlive_UKMSR <- msCohort_UKMSR %>%
  filter(ALF_PE %in% stillInWales$ALF_PE&Dead==0)

save(msCohortAlive_UKMSR,file=paste(WORKING_DIR,COHORT_DIR,"MS Cohort (V2 - UKMSR) Alive.RData",sep=""))

demyeOnlyUsers <- cleanPopulationTable %>%
  filter(is.na(MSHospitalOnsetDate)&!is.na(PotentialMSOnsetDate)) %>%
  rename(CohortDiagAge=PotentialMSOnsetAge)

save(demyeOnlyUsers,file=paste(WORKING_DIR,COHORT_DIR,"Demye Only Users.RData",sep=""))

demyeOnlyUsers_Alive <- demyeOnlyUsers %>%
  filter(ALF_PE %in% stillInWales$ALF_PE&Dead==0)

save(demyeOnlyUsers_Alive,file=paste(WORKING_DIR,COHORT_DIR,"Demye Only Users Alive.RData",sep=""))

#####
  
msAllExtraCounts <- extraMSEvents_MasterFrame %>%
  mutate(MSOnlyCount=MSGPEventCount+MSHospitalEventCount) %>%
  mutate(OpticNCount=OpticNGPEventCount+OpticNHospitalEventCount) %>%
  mutate(TransMyeCount=TransMyeGPEventCount+TransMyeHospitalEventCount) %>%
  mutate(EncephCount=EncephGPEventCount+EncephHospitalEventCount) %>%
  mutate(CNSDisCount=CNSDisGPEventCount+CNSDisHospitalEventCount) %>%
  mutate(AccDemyeCount=AccDemyeGPEventCount+AccDemyeHospitalEventCount) %>%
  mutate(NMOEventCount=NMOGPEventCount+NMOHospitalEventCount) %>%
  select(ALF_PE,Gender,MSOnlyCount,OpticNCount,TransMyeCount,EncephCount,CNSDisCount,AccDemyeCount,NMOEventCount)

totals <- msAllExtraCounts %>%
  filter(ALF_PE %in% cleanPopulationTable$ALF_PE) %>%
  select(-Gender) %>%
  pivot_longer(names_to = "Type",values_to="Total",cols=ends_with("Count")) %>%
  filter(Total>0) %>%
  mutate(Total=if_else(Total>1,1,Total))

totalsMissing <- msAllExtraCounts %>%
  filter(ALF_PE %in% demyeOnlyUsers$ALF_PE) %>%
  select(-Gender) %>%
  pivot_longer(names_to = "Type",values_to="Total",cols=ends_with("Count")) %>%
  filter(Total>0) %>%
  mutate(Total=if_else(Total>1,1,Total))

totalsTable <- totals %>%
  group_by(Type) %>%
  summarise(NumberOfPeople=sum(Total)) %>%
  ungroup() %>%
  rbind(data.frame(Type="Grand Total (All MS)",NumberOfPeople=length(unique(totals$ALF_PE))))

####
totals_Alive <- msAllExtraCounts %>%
  filter(ALF_PE %in% cleanPopulationTable_Alive$ALF_PE) %>%
  select(-Gender) %>%
  pivot_longer(names_to = "Type",values_to="Total",cols=ends_with("Count")) %>%
  filter(Total>0) %>%
  mutate(Total=if_else(Total>1,1,Total))

totalsMissing_Alive <- msAllExtraCounts %>%
  filter(ALF_PE %in% demyeOnlyUsers_Alive$ALF_PE) %>%
  select(-Gender) %>%
  pivot_longer(names_to = "Type",values_to="Total",cols=ends_with("Count")) %>%
  filter(Total>0) %>%
  mutate(Total=if_else(Total>1,1,Total))

totalsTable_Alive <- totals_Alive %>%
  group_by(Type) %>%
  summarise(NumberOfPeople=sum(Total)) %>%
  ungroup() %>%
  rbind(data.frame(Type="Grand Total (All MS)",NumberOfPeople=length(unique(totals_Alive$ALF_PE))))

totalsTable_Missing_Alive <- totalsMissing_Alive %>%
  group_by(Type) %>%
  summarise(NumberOfPeople=sum(Total)) %>%
  ungroup()

####

totalsTable_Missing <- totalsMissing %>%
  group_by(Type) %>%
  summarise(NumberOfPeople=sum(Total)) %>%
  ungroup()

totalsPeople <- msAllExtraCounts %>%
  filter(ALF_PE %in% msCohort_UKMSR$ALF_PE) %>%
  select(ALF_PE) %>% distinct()

write.csv(totalsTable,file=paste(WORKING_DIR,OUT_DIR,"Total Extra Codes (Everyone).csv",sep=""),row.names=FALSE)
write.csv(totalsTable_Alive,file=paste(WORKING_DIR,OUT_DIR,"Total Extra Codes (Alive).csv",sep=""),row.names=FALSE)
write.csv(totalsTable_Missing,file=paste(WORKING_DIR,OUT_DIR,"Demye Only Code Counts (Everyone).csv",sep=""),row.names=FALSE)
write.csv(totalsTable_Missing_Alive,file=paste(WORKING_DIR,OUT_DIR,"Demye Only Code Counts (Alive).csv",sep=""),row.names=FALSE)

save(msAllExtraCounts,file=paste(WORKING_DIR,COHORT_DIR,"MS Extra Count Frames.RData",sep=""))
