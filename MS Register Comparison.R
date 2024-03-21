#Comparing the MS Register with the SAIL Databank MS Cohort

library(tidyverse)
library(lubridate)
library("GetoptLong")
library(compareGroups)

#Some of this script runs queries. They're saved in RData files to avoid having to reload
#SQL connections constantly. If you need refreshed frames set to TRUE
RUN_INITIAL_QUERIES <- FALSE

#Main Project Working Directory
WORKING_DIR <- "S:/0945 - UK MS Register/msincidence (JNNP Final)/"
source(paste(WORKING_DIR,"Define File Constants.R",sep=""))

#SQL Queries
if(RUN_INITIAL_QUERIES==TRUE)
{
  #Establish connection
  sail <- SAILDBUtils::SAILConnect()
  MYMS_DATABASE <- paste(PROJECT_NUMBER,".MYMS_20201123",sep="")
  REGTOALF_DATABASE <- paste(PROJECT_NUMBER,".MS_ALF_20200108",sep="")
  
  MSREG_DATEWINDOW <- '20220713'
  REGTOALFDMT_DATABASE <- paste(PROJECT_NUMBER,".UKMS_MSREGISTER_ALF_",MSREG_DATEWINDOW,sep="")
  
  #Get ALF information of all UK MS Register users in SAIL that are resident in Wales
  msRegisterCohort_Wales <- SAILDBUtils::runSQL(sail,qq("SELECT DISTINCT ALF_PE,WOB,GNDR_CD 
                                            FROM @{REGTOALF_DATABASE} 
                                            WHERE ALF_PE IS NOT NULL
                                            AND LSOA2001_CD LIKE 'W%'
                                            AND ALF_STS_CD IN (1,4,39)"),strings_as_factors = FALSE)
  
  #Remove any potential duplicate ALF keys.
  msRegisterCohort_Duplicates <- msRegisterCohort_Wales[duplicated(msRegisterCohort_Wales$ALF_PE),]
  msRegisterCohort_Wales <- msRegisterCohort_Wales %>%
    filter(!ALF_PE %in% msRegisterCohort_Duplicates$ALF_PE)
  
  #Save the UK MS Register Cohort
  save(msRegisterCohort_Wales,file=paste(WORKING_DIR,RAWSQL_DIR,"MS Register Cohort (Wales).RData",sep=""))
  
  msRegisterCohort_All <- SAILDBUtils::runSQL(sail,qq("SELECT DISTINCT ALF_PE,WOB,GNDR_CD 
                                            FROM @{REGTOALF_DATABASE} 
                                            WHERE ALF_PE IS NOT NULL
                                            AND ALF_STS_CD IN (1,4,39)"),strings_as_factors = FALSE)
  
  msRegisterCohort_Duplicates <- msRegisterCohort_All[duplicated(msRegisterCohort_All$ALF_PE),]
  msRegisterCohort_All <- msRegisterCohort_All %>%
    filter(!ALF_PE %in% msRegisterCohort_Duplicates$ALF_PE)
  
  save(msRegisterCohort_All,file=paste(WORKING_DIR,RAWSQL_DIR,"MS Register Cohort (All).RData",sep=""))
  
  #Get MS Symptom Data
  myMSSymp <- SAILDBUtils::runSQL(sail,qq("SELECT
    USERID_PE,
    MSONSET_YR,
    MSONSET_MONTH,
    CASE WHEN MSONSET_MONTH>0 AND MSONSET_MONTH<10
  	  AND MSONSET_YR>0 AND MSONSET_YR<=@{YEAR_MAX}
  	  THEN MSONSET_YR || '-0' || MSONSET_MONTH || '-01'
  	  WHEN MSONSET_MONTH>=10 AND MSONSET_MONTH<=12
  	  AND MSONSET_YR>0 AND MSONSET_YR<=@{YEAR_MAX}
  	  THEN MSONSET_YR || '-' || MSONSET_MONTH || '-01'
  	  WHEN MSONSET_MONTH=0
  	  AND MSONSET_YR>0 AND MSONSET_YR<=@{YEAR_MAX}
  	  THEN MSONSET_YR || '-01-01'
  	ELSE NULL
  	END AS MSONSET_FORMATTED,
    COMPLETEDDATE,
    GROUPID
    FROM(
      SELECT USERID_PE,
        COMPLETEDDATE,
        GROUPID,
        CASE WHEN V3_SYMPTOMS_YEAR='NA' OR TRANSLATE(V3_SYMPTOMS_YEAR,'X ', ' 0123456789') <> '' THEN NULL
  		    ELSE CAST(V3_SYMPTOMS_YEAR AS INT)
		    END AS MSONSET_YR,
  		  CASE WHEN V3_SYMPTOMS_MONTH='NA' OR TRANSLATE(V3_SYMPTOMS_MONTH,'X ', ' 0123456789') <> '' THEN NULL
  		    ELSE CAST(V3_SYMPTOMS_MONTH AS INT)
		    END AS MSONSET_MONTH
		    FROM @{MYMS_DATABASE}
    )MSOnset"),strings_as_factors=FALSE)
  
  save(myMSSymp,file=paste(WORKING_DIR,RAWSQL_DIR,"MS Onset Responses.RData",sep=""))
  
  #Get the Latest reported MS Type Now Responses
  myMSNow <- SAILDBUtils::runSQL(sail,qq("SELECT
    USERID_PE,
  	CASE WHEN MSTYPENOW='0' THEN 'PPMS'
  		WHEN MSTYPENOW='1' THEN 'RRMS'
  		WHEN MSTYPENOW='2' THEN 'SPMS'
  		WHEN MSTYPENOW='4' THEN 'Benign'
  		WHEN MSTYPENOW='3' THEN 'Unknown'
  		ELSE NULL
  	END AS MSTYPE_LATEST,
  	MSNOW_YR,
  	MSNOW_MONTH,
  	CASE WHEN MSNOW_MONTH>0 AND MSNOW_MONTH<10
  	  AND MSNOW_YR>0 AND MSNOW_YR<=@{YEAR_MAX}
  	  THEN MSNOW_YR || '-0' || MSNOW_MONTH || '-01'
  	  WHEN MSNOW_MONTH>=10 AND MSNOW_MONTH<=12
  	  AND MSNOW_YR>0 AND MSNOW_YR<=@{YEAR_MAX}
  	  THEN MSNOW_YR || '-' || MSNOW_MONTH || '-01'
  	  WHEN MSNOW_MONTH=0
  	  AND MSNOW_YR>0 AND MSNOW_YR<=@{YEAR_MAX}
  	  THEN MSNOW_YR || '-01-01'
  	ELSE NULL
  	END AS MSNOW_FORMATTED,
  	COMPLETEDDATE,
  	GROUPID
    FROM(
  	  SELECT USERID_PE,
  		  COMPLETEDDATE,
  		  GROUPID,
  		  CASE WHEN V3_DK_MSTYPE_NOW!='NA' THEN V3_DK_MSTYPE_NOW
  			  WHEN V3_PP_MSTYPE_NOW!='NA' THEN V3_PP_MSTYPE_NOW
  			  WHEN V3_RR_MSTYPE_NOW!='NA' THEN V3_RR_MSTYPE_NOW
  			  WHEN V3_SP_MSTYPE_NOW!='NA' THEN V3_SP_MSTYPE_NOW
  			  ELSE NULL 
  		  END AS MSTYPENOW,
  		  CASE WHEN V3_MSTYPE_NOW_DATE='NA' OR TRANSLATE(V3_MSTYPE_NOW_DATE,'X ', ' 0123456789') <> '' THEN NULL
  		    ELSE CAST(V3_MSTYPE_NOW_DATE AS INT)
		    END AS MSNOW_YR,
  		  CASE WHEN V3_MSTYPE_NOW_MONTH='NA' OR TRANSLATE(V3_MSTYPE_NOW_MONTH,'X ', ' 0123456789') <> '' THEN NULL
  		    ELSE CAST(V3_MSTYPE_NOW_MONTH AS INT)
		    END AS MSNOW_MONTH
  	  FROM @{MYMS_DATABASE}
    )MSTypeNowResponses"),strings_as_factors=FALSE)
  
  save(myMSNow,file=paste(WORKING_DIR,RAWSQL_DIR,"MS Type Now Responses.RData",sep=""))
  
  #Get the Latest Reported MS Type Diagnosis Responses
  myMSDiag <- SAILDBUtils::runSQL(sail,qq("SELECT USERID_PE,
	CASE WHEN MSTYPEDIAG='0' THEN 'PPMS'
		WHEN MSTYPEDIAG='1' THEN 'RRMS'
		WHEN MSTYPEDIAG='2' THEN 'SPMS'
		WHEN MSTYPEDIAG='3' THEN 'Unknown'
		ELSE NULL
	END AS MSTYPEDIAG_LATEST,
	MSDIAG_YR,
	MSDIAG_MONTH,
	CASE WHEN MSDIAG_MONTH>0 AND MSDIAG_MONTH<10 AND MSDIAG_YR>0 AND MSDIAG_YR<=2020 THEN MSDIAG_YR || '-0' || MSDIAG_MONTH || '-01'
		WHEN MSDIAG_MONTH>=10 AND MSDIAG_MONTH<=12 AND MSDIAG_YR>0 AND MSDIAG_YR<=2020 THEN MSDIAG_YR || '-' || MSDIAG_MONTH || '-01'
		WHEN (MSDIAG_MONTH=0 OR MSDIAG_MONTH IS NULL) AND MSDIAG_YR>0 AND MSDIAG_YR<=2020 THEN MSDIAG_YR || '-01-01'
		ELSE NULL
	END AS MSDIAG_FORMATTED,
	COMPLETEDDATE,
	GROUPID
	FROM(
		SELECT USERID_PE,
			COMPLETEDDATE,
			GROUPID,
			CASE WHEN V3_YR_DIAGNOSIS='NA' OR TRANSLATE(V3_YR_DIAGNOSIS,'X ', ' 0123456789') <> '' THEN NULL
				ELSE CAST(V3_YR_DIAGNOSIS AS INT)
			END AS MSDIAG_YR,
			CASE WHEN V3_MONTH_DIAGNOSIS='NA' OR TRANSLATE(V3_MONTH_DIAGNOSIS,'X ', ' 0123456789') <> '' THEN NULL
				ELSE CAST(V3_MONTH_DIAGNOSIS AS INT)
			END AS MSDIAG_MONTH,
			CASE WHEN V3_MS_AT_DIAGNOSIS!='NA' THEN V3_MS_AT_DIAGNOSIS
				ELSE NULL
			END AS MSTYPEDIAG
		FROM @{MYMS_DATABASE}
	)MSTypeDiagResponses"),strings_as_factors=FALSE)
  
  save(myMSDiag,file=paste(WORKING_DIR,RAWSQL_DIR,"MS Type Diag Responses.RData",sep=""))
  
  load(paste(WORKING_DIR,COHORT_DIR,"Portal Demographics (Reformatted).RData",sep=""))
  
  #Close the connection when finished
  SAILDBUtils::close_connection(sail)
}else
{
  #If the query has already been run, load the query results up
  load(paste(WORKING_DIR,RAWSQL_DIR,"MS Type Diag Responses.RData",sep=""))
  load(paste(WORKING_DIR,RAWSQL_DIR,"MS Type Now Responses.RData",sep=""))
  load(paste(WORKING_DIR,RAWSQL_DIR,"MS Onset Responses.RData",sep=""))
  load(paste(WORKING_DIR,RAWSQL_DIR,"MS Register Cohort (All).RData",sep=""))
  load(paste(WORKING_DIR,RAWSQL_DIR,"MS Register Cohort (Wales).RData",sep=""))
  load(paste(WORKING_DIR,COHORT_DIR,"Portal Demographics (Reformatted).RData",sep=""))
}

#Load the cohorts
source(paste(WORKING_DIR,"Load Cohorts.R",sep=""))

msCohortList <- list()
msCohortList[[1]] <- msCohort_UKMSR

#Merge the Total Population cohort with the register data to see who exists in the GP Data
cleanPopulation_MSRegisterWales <- cleanPopulationTable %>% 
  filter(ALF_PE %in% msRegisterCohort_Wales$ALF_PE)

#Just a quick check to see how many come back including those not in Wales
cleanPopulation_MSRegisterAll <- cleanPopulationTable %>%
  filter(ALF_PE %in% msRegisterCohort_All$ALF_PE)

#Get the Latest MS Type Now Info from Register
myMSNow_Users <- myMSNow %>%
  left_join(portalDemo,by="USERID_PE") %>%
  #If they're not in the Wales SAIL cohort give a negative integer so joins won't work
  #(But can still be merged with other portal data)
  mutate(ALF_PE=if_else(is.na(ALF_PE),-USERID_PE,ALF_PE)) %>%
  arrange(ALF_PE,desc(COMPLETEDDATE),desc(GROUPID)) %>%
  group_by(ALF_PE) %>%
  mutate(rowid=row_number()) %>%
  ungroup() %>%
  filter(rowid==1) %>%
  select(-rowid) %>%
  mutate(MSTYPE_LATEST=if_else(is.na(MSTYPE_LATEST)|MSTYPE_LATEST=="Unknown","Unknown",MSTYPE_LATEST)) %>%
  mutate(MSTYPE_LATEST=if_else(MSTYPE_LATEST %in% c("Benign","RRMS"),"Benign/RRMS",MSTYPE_LATEST)) %>%
  mutate(MSNOW_FORMATTED=as.Date(MSNOW_FORMATTED,"%Y-%m-%d")) %>%
  filter(is.na(MSNOW_FORMATTED)|MSNOW_FORMATTED<=DATE_MAX)

#Get the Latest MS Onset info from Register
myMSSymp_Users <- myMSSymp %>%
  left_join(portalDemo,by="USERID_PE") %>%
  #If they're not in the Wales SAIL cohort give a negative integer so joins won't work
  #(But can still be merged with other portal data)
  mutate(ALF_PE=if_else(is.na(ALF_PE),-USERID_PE,ALF_PE)) %>%
  arrange(ALF_PE,desc(COMPLETEDDATE),desc(GROUPID)) %>%
  group_by(ALF_PE) %>%
  mutate(rowid=row_number()) %>%
  ungroup() %>%
  filter(rowid==1) %>%
  select(-rowid) %>%
  mutate(MSONSET_FORMATTED=as.Date(MSONSET_FORMATTED,"%Y-%m-%d")) %>%
  filter(is.na(MSONSET_FORMATTED)|MSONSET_FORMATTED<=DATE_MAX) %>%
  mutate(AgeOnsetMSRegister=if_else(MSONSET_FORMATTED>=WOB,as.numeric(round((MSONSET_FORMATTED-WOB)/365.25,digits=2)),NA_real_)) %>%
  mutate(AgeOnsetMSRegister_N=if_else(!is.na(AgeOnsetMSRegister),"Present","Missing"))

#Get the Latest MS Diagnosis info from Register
myMSDiag_Users <- myMSDiag %>%
  left_join(portalDemo,by="USERID_PE") %>%
  #If they're not in the Wales SAIL cohort give a negative integer so joins won't work
  #(But can still be merged with other portal data)
  mutate(ALF_PE=if_else(is.na(ALF_PE),-USERID_PE,ALF_PE)) %>%
  arrange(ALF_PE,desc(COMPLETEDDATE),desc(GROUPID)) %>%
  group_by(ALF_PE) %>%
  mutate(rowid=row_number()) %>%
  ungroup() %>%
  filter(rowid==1) %>%
  select(-rowid) %>%
  mutate(MSTYPEDIAG_LATEST=if_else(is.na(MSTYPEDIAG_LATEST)|MSTYPEDIAG_LATEST=="Unknown","Unknown",MSTYPEDIAG_LATEST)) %>%
  mutate(MSDIAG_FORMATTED=as.Date(MSDIAG_FORMATTED,"%Y-%m-%d")) %>%
  filter(is.na(MSDIAG_FORMATTED)|MSDIAG_FORMATTED<=DATE_MAX) %>%
  mutate(AgeDiagMSRegister=if_else(MSDIAG_FORMATTED>=WOB,as.numeric(round((MSDIAG_FORMATTED-WOB)/365.25,digits=2)),NA_real_)) %>%
  mutate(AgeDiagMSRegister_N=if_else(!is.na(AgeDiagMSRegister),"Present","Missing"))

#This is used to determine diagnosis during the study period for the Cardiff flowchart
myMSDiag_StudyPeriod <- myMSDiag_Users %>%
  filter(ALF_PE %in% msRegisterCohort_Wales$ALF_PE&MSDIAG_FORMATTED>='1970-01-01'&MSDIAG_FORMATTED<='2020-01-01')

#####
#Join the UK MS Register Cohort with the questionnaire results to determine their current status 
myMSNow_All <- cleanPopulation_MSRegisterAll %>%
  left_join(myMSNow_Users %>% select(ALF_PE,MSTYPE_LATEST,MSNOW_FORMATTED),by="ALF_PE") %>%
  mutate(MSTYPE_LATEST=if_else(is.na(MSTYPE_LATEST)|MSTYPE_LATEST=="Unknown","Unknown",MSTYPE_LATEST)) %>%
  left_join(myMSSymp_Users %>% select(ALF_PE,MSONSET_FORMATTED,AgeOnsetMSRegister,AgeOnsetMSRegister_N),by="ALF_PE") %>%
  left_join(myMSDiag_Users %>% select(ALF_PE,MSTYPEDIAG_LATEST,MSDIAG_FORMATTED,AgeDiagMSRegister,AgeDiagMSRegister_N,COMPLETEDDATE) %>% rename(MyMSSubmission=COMPLETEDDATE),by="ALF_PE") %>%
  mutate(MSTYPEDIAG_LATEST=if_else(is.na(MSTYPEDIAG_LATEST)|MSTYPEDIAG_LATEST=="Unknown","Unknown",MSTYPEDIAG_LATEST))

myMSNow_Unfiltered <- myMSNow_All
save(myMSNow_Unfiltered,file=paste(WORKING_DIR,OUT_DIR,"UKMSR My MS (Unfiltered).RData",sep=""))

myMSNow_All <- myMSNow_All %>%
  filter(MSDIAG_FORMATTED>="1970-01-01"&MSDIAG_FORMATTED<="2020-01-01")
  
save(myMSNow_All,file=paste(WORKING_DIR,OUT_DIR,"UKMSR My MS (Diagnosis Filtered).RData",sep=""))

#Determine the latest MS Type of the MS Register Cohort
myMSNow_AllCounts <- myMSNow_All %>%
  count(MSTYPE_LATEST)

myMSDiag_AllCounts <- myMSNow_All %>%
  count(MSTYPEDIAG_LATEST)

#Function to determine which Register users match up with the UKMSR/Manitoba cohorts
getMatches <- function(cohort,groupName,diagOnlyFlag=FALSE)
{
  #Number flag show the N shows on CompareGroups
  cohort_WithN <- cohort %>%
    mutate(CohortDiagAge_N=if_else(!is.na(CohortDiagAge),"Present","Missing"))
  
  #Match the Register patients with the cohort selected. These are our found patients
  matchingPatients <- myMSNow_All %>%
    inner_join(cohort_WithN %>% select(ALF_PE,CohortDiagYear,CohortDiagAge,CohortDiagAge_N),by="ALF_PE") %>%
    mutate(SubGroup="Disease Present and Tested Positive")
  
  #Anyone who isn't in the cohort is missing
  missingPatients <- myMSNow_All %>%
    filter(!ALF_PE %in% cohort$ALF_PE) %>%
    mutate(CohortDiagAge=NA_real_) %>%
    mutate(CohortDiagYear=NA_real_) %>%
    mutate(CohortDiagAge_N="Missing") %>%
    mutate(SubGroup="Disease Present and Tested Negative")
  
  #If we're only interested in register patients with a diagnosis, filter
  #the matching/missing patients to do so
  if(diagOnlyFlag==TRUE)
  {
    matchingPatients <- matchingPatients %>%
      filter(MSTYPEDIAG_LATEST!="Unknown")
    
    missingPatients <- missingPatients %>%
      filter(MSTYPEDIAG_LATEST!="Unknown")
  }
  
  #Output the result frame according to the cohort used
  resultFrame <- matchingPatients %>%
    rbind(missingPatients) %>%
    mutate(Group=groupName) %>%
    mutate(AllGroup=paste(Group,SubGroup,sep=" - "))
  
  return(resultFrame)
}

#Getting an overall demographic view of everyone on the UKMS Register portal
validUKMSCohort <- portalDemo %>%
  mutate(ALF_PE=if_else(is.na(ALF_PE),-USERID_PE,ALF_PE)) %>%
  mutate(Gender=case_when(GNDR_CD=="1" ~ "Male",
                          GNDR_CD=="2" ~ "Female",
                          TRUE ~ "Unknown")) %>%
  left_join(myMSNow_Users %>% select(ALF_PE,MSTYPE_LATEST,MSNOW_FORMATTED),by="ALF_PE") %>%
  left_join(myMSDiag_Users %>% select(ALF_PE,MSTYPEDIAG_LATEST,MSDIAG_FORMATTED,AgeDiagMSRegister),by="ALF_PE") %>%
  left_join(myMSSymp_Users %>% select(ALF_PE,MSONSET_FORMATTED,AgeOnsetMSRegister,AgeOnsetMSRegister_N),by="ALF_PE") %>%
  mutate(InvalidUser=if_else(!is.na(MSDIAG_FORMATTED)&!is.na(WOB)&WOB!="1900-01-01"&GNDR_CD %in% c(1,2),0,1,missing=1)) %>%
  select(ALF_PE,WOB,Gender,
         MSONSET_FORMATTED,AgeOnsetMSRegister,AgeOnsetMSRegister_N,
         MSTYPE_LATEST,MSTYPEDIAG_LATEST,MSDIAG_FORMATTED,AgeDiagMSRegister,
         InvalidUser) %>%
  mutate(AgeDiagMSRegister_N="Present") %>%
  mutate(Group="UKMSR Website") %>%
  distinct() %>%
  group_by(ALF_PE) %>%
  filter(n()==1) %>%
  ungroup()

validUKMSCohort_SensibleDemo <- validUKMSCohort %>%
  filter(InvalidUser==0)

#####
#Generate a group to show demographics of everyone on the UKMS Register portal
ms_All <- myMSNow_All %>%
  mutate(CohortDiagYear=NA_real_) %>%
  mutate(CohortDiagAge=NA_real_) %>%
  mutate(CohortDiagAge_N="Missing") %>%
  mutate(SubGroup="") %>%
  mutate(Group="All UKMSR Population") %>%
  mutate(AllGroup=Group)

#Find out how many from the UKMSR/Manitoba Methods match up to those on the UKMS Register portal
ukmsrResults_All <- getMatches(msCohortList[[1]], "UKMSR")
save(ukmsrResults_All,file=paste(WORKING_DIR,OUT_DIR,"UKMSR Matches.RData",sep=""))

#Combine the accuracy results of UKMSR/Manitoba methods with the whole demographic
allResults <- ukmsrResults_All %>%
  rbind(ms_All) %>%
  #Need to group up the MS Types to ensure counts are sufficient
  mutate(MSTYPE_LATEST_GROUP=if_else(MSTYPE_LATEST %in% c("PPMS","SPMS"), "Progressive",MSTYPE_LATEST)) %>%
  mutate(MSTYPEDIAG_LATEST_GROUP=if_else(MSTYPEDIAG_LATEST %in% c("PPMS","SPMS"),"Progressive",MSTYPEDIAG_LATEST))

#####
#Determining demographics for Table 1
comparisonTable <- cleanPopulationTable %>%
  select(ALF_PE,Gender,Dead,AgeEnd,AgeDeath) %>%
  mutate(MSGPEventCount=NA_real_,
         MSHospitalEventCount=NA_real_) %>%
  mutate(CohortDiagAge=NA_real_) %>%
  mutate(Group="All Population") %>%
  rbind(demyeOnlyUsers %>%
          select(ALF_PE,Gender,Dead,AgeEnd,AgeDeath,CohortDiagAge) %>% 
          mutate(MSGPEventCount=NA_real_,MSHospitalEventCount=NA_real_) %>%
          mutate(Group="Demye Only Users")) %>%
  rbind(myMSNow_All %>% 
          mutate(CohortDiagAge=AgeDiagMSRegister) %>% 
          select(ALF_PE,Gender,Dead,AgeEnd,AgeDeath,CohortDiagAge) %>% 
          mutate(MSGPEventCount=NA_real_,MSHospitalEventCount=NA_real_) %>%
          mutate(Group="UKMSR SAIL Cohort")) %>%
  rbind(msCohort_UKMSR %>% 
          select(ALF_PE,Gender,Dead,AgeEnd,AgeDeath,CohortDiagAge,MSGPEventCount,MSHospitalEventCount) %>%
          mutate(Group="MS (UKMSR)")) %>%
  select(ALF_PE,Group,CohortDiagAge,AgeDeath,AgeEnd,Gender,Dead,MSGPEventCount,MSHospitalEventCount) %>%
  mutate(Dead=factor(Dead,levels=c(0,1),labels=c("Alive","Dead")))

resTable1 <- compareGroups(Group ~ CohortDiagAge+AgeDeath+AgeEnd+Gender+Dead+MSGPEventCount+MSHospitalEventCount,comparisonTable)
table1Results <- createTable(resTable1,show.p.overall = FALSE)
export2csv(table1Results,file=paste(WORKING_DIR,TESTOUT_DIR,"Table 1.csv",sep=""))

#Comparing demographics of UKMSR/Manitoba method classifications against
#those on the UKMSR that can be linked to SAIL
compareAccuracyDemo <- function(resultCohort,groups,fileName)
{
  resAllGroups <- compareGroups(AllGroup ~ Gender
                                +MSTYPEDIAG_LATEST_GROUP
                                +MSTYPE_LATEST_GROUP
                                +AgeOnsetMSRegister_N+AgeOnsetMSRegister
                                +CohortDiagAge_N+CohortDiagAge
                                +AgeDiagMSRegister_N+AgeDiagMSRegister
                                +AgeEnd+AgeDeath,
                                resultCohort %>% filter(Group %in% groups),
                                method=c(3,
                                         3,
                                         3,
                                         3,1,
                                         3,1,
                                         3,1,
                                         1,1))
  
  tabAllGroups <- createTable(resAllGroups,hide.no="Missing")
  export2csv(tabAllGroups,file=paste(WORKING_DIR,TESTOUT_DIR,fileName,sep=""))
}

compareAccuracyDemo(allResults,c("All UKMSR Population","UKMSR"), "MS Resister Results.csv")