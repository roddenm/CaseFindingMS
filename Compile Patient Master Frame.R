library("SAILDBUtils")
library("GetoptLong")
library(tidyverse)
library(lubridate)

#####
#Options
#####

#Run the initial queries on SAIL?
RUN_INITIAL_QUERIES <- FALSE
DOWNLOAD_ALL_PATIENTS <- FALSE
DOWNLOAD_ALL_DEATHS <- FALSE

#Working Directory (Where you're using the script)
WORKING_DIR <- "S:/0945 - UK MS Register/msincidence (JNNP Final)/"
source(paste(WORKING_DIR,"Define File Constants.R",sep=""))

#Views on the project are set by date windows
#If you wish to use an earlier/later view for a particular dataset change it here
GP_DATEWINDOW <- "20210301"
ADDE_DATEWINDOW <- "20210328"
WDSD_DATEWINDOW <- "20230403"
PEDW_DATEWINDOW <- "20210328"
PATH_DATEWINDOW <- "20220805"
OPDW_DATEWINDOW <- "20210328"

#Using an older view for WDSD deaths for consistency, locations were derived from an updated view
WDSD_OLD_DATEWINDOW <- "20210328"

#The databases we're interested in
#If things are set correctly in file constant script, this should set to the appropriate project

#GP Database
#GP Events (Where all READ codes are stored)
GP_EVENT_DATABASE <- paste(PROJECT_NUMBER,".WLGP_GP_EVENT_CLEANSED_",GP_DATEWINDOW,sep="")
#GP Patients (Where patient demographics are held)
GP_PATIENT_DATABASE <- paste(PROJECT_NUMBER,".WLGP_PATIENT_ALF_CLEANSED_",GP_DATEWINDOW,sep="")
#GP Reg Database (GP Registration Periods)
GP_REG_DATABASE <- paste(PROJECT_NUMBER,".WLGP_CLEAN_GP_REG_BY_PRAC_INCLNONSAIL_MEDIAN_",GP_DATEWINDOW,sep="")
#Annual District Death Extract (Dates of Death)
ADDE_DATABASE <- paste(PROJECT_NUMBER,".ADDE_DEATHS_",ADDE_DATEWINDOW,sep="")
#Welsh Demographics Service (Dates of Death)
WDSD_DATABASE <- paste(PROJECT_NUMBER,".WDSD_AR_PERS_",WDSD_OLD_DATEWINDOW,sep="")

WDSD_WALES <- paste(PROJECT_NUMBER,".WDSD_SINGLE_CLEAN_GEO_WALES_",WDSD_DATEWINDOW,sep="")
#Patient Episode Data Wales (Hospital Data)
#Hosptial Data organised into Spells (Stay in hospital), Episode (Events during the stay)
#And diagnosis (ICD10 codes during events)
PEDW_SPELL <- paste(PROJECT_NUMBER,".PEDW_SPELL_",PEDW_DATEWINDOW,sep="")
PEDW_EPISODE <- paste(PROJECT_NUMBER,".PEDW_EPISODE_",PEDW_DATEWINDOW,sep="")
PEDW_DIAG <- paste(PROJECT_NUMBER,".PEDW_DIAG_",PEDW_DATEWINDOW,sep="")

#Outpatient Dataset
OPDW_OUTPATIENT <- paste(PROJECT_NUMBER,".OPDW_OUTPATIENTS_",OPDW_DATEWINDOW,sep="")
OPDW_DIAG <- paste(PROJECT_NUMBER,".OPDW_OUTPATIENTS_DIAG_",OPDW_DATEWINDOW,sep="")

#Pathology Results (Currently used for EBV results)
PATH_DATABASE <- paste(PROJECT_NUMBER,".WRRS_OBSERVATION_RESULT_",PATH_DATEWINDOW,sep="")
#BMI Table Generated using the BMI Project on the Shared Analyst Gitlab (Project 0000)
#You may need to file a helpdesk ticket to get access to project 0000
BMI_DATABASE <- paste(PROJECT_WORK_NUMBER,".HD_BMI_FINAL",sep="")
#Table to store all ALF keys with MS so we can grab all BMI entries for them
#BMI table is too big to grab everything in one go
ALLMS_DATABASE <- paste(PROJECT_WORK_NUMBER,".ALL_MS_COHORT",sep="")
#Diabetes Table from the Diabetes Project on the Shared Analyst Gitlab (Project 0000)
DIAB_DATABASE <- paste(PROJECT_WORK_NUMBER,".DIABETES_COHORT",sep="")
#Welsh Demographics Service Locations (Used to gather health board info)
WDSD_LOC_DATABASE <- paste(PROJECT_NUMBER,".WDSD_SINGLE_CLEAN_GEO_CHAR_LSOA2011_",WDSD_DATEWINDOW,sep="")
WDSD_RALF_DATABASE <- paste(PROJECT_NUMBER,".WDSD_SINGLE_CLEAN_GEO_RALF_LSOA2011_",WDSD_DATEWINDOW,sep="")
#MS Register Demographics
REGTOALF_DATABASE <- paste(PROJECT_NUMBER,".MS_ALF_20200108",sep="")

#####
#Functions
#####

#Get Event Count
#This is just a bit of a sanity check to make sure that what you're downloading from the events database is
#reasonably sized, as R's SQL library doesn't have brakes. It will happily carry on until it hits a memory problem.
getEventCount <- function(eventCodes,sailConnection)
{
  codesForQuery <- paste(as.character(eventCodes),collapse="','")
  
  countSanityCheck <- SAILDBUtils::runSQL(sailConnection,qq("SELECT COUNT(*) AS NumberOfEvents FROM @{GP_EVENT_DATABASE}
                                                  WHERE EVENT_CD IN ('@{codesForQuery}')
                                                  AND ALF_PE IS NOT NULL AND EVENT_YR >= @{YEAR_MIN}
                                                  AND EVENT_YR<= @{YEAR_MAX}"), echo=TRUE, strings_as_factors=FALSE)
  
  return(countSanityCheck$NUMBEROFEVENTS[1])
}

#Get Specific Events
getEvents <- function(eventCodes,sailConnection)
{
  #Check how many events are being returned before getting individual rows
  eventCount <- getEventCount(eventCodes,sailConnection)
  
  #This checks to make sure that a crazy number of events aren't being returned.
  #This can be increased but bear in mind the query will take a long time and you 
  #may run into memory issues.
  if(eventCount>900000)
  {
    cat("There are a lot of events being returned! Stopping!\n")
    cat("Actual Event Count =",eventCount,"\n")
    stop("Script Stopped!")
  }
  else{
    cat("Event Count =",eventCount,"\n")
    codesForQuery <- paste(as.character(eventCodes),collapse="','")
    events <- SAILDBUtils::runSQL(sailConnection, qq("SELECT ALF_PE,EVENT_DT,
                                           EVENT_YR,EVENT_CD,EVENT_CD_VRS,EVENT_VAL,EPISODE,SEQUENCE 
                                           FROM @{GP_EVENT_DATABASE}
                                           WHERE EVENT_CD IN ('@{codesForQuery}') 
                                           AND ALF_PE IS NOT NULL AND EVENT_YR >= @{YEAR_MIN}
                                           AND EVENT_YR<= @{YEAR_MAX}"), echo=FALSE, strings_as_factors=FALSE)
    return(events)
  }
}

#Filter an event frame to just the patients we're interested in
preProcessEvents <- function(eventFrame,validPatientFrame)
{
  validPatients <- eventFrame[eventFrame$ALF_PE %in% validPatientFrame$ALF_PE,]
  validPatients_Gender <- validPatients %>% left_join(validPatientFrame[,c("ALF_PE","WOB","GNDR_CD")],by="ALF_PE")
  return(validPatients_Gender)
}
####

#Get All Valid Patients
if(RUN_INITIAL_QUERIES == TRUE)
{
  #Establish Connection (A window will pop up asking for your SAIL details)
  sail <- SAILDBUtils::SAILConnect()
  
  ####
  #Getting All Patients takes a while. Don't run unless you absolutely have to!
  #All Patients in GP Data Set
  if(DOWNLOAD_ALL_PATIENTS==TRUE)
  {
    #Getting all patients from the GP dataset
    #ALF_STS_CD refers to how the Anonymised Linking Field was allocated to the data
    #Full details are at R:/SAIL Reference Library/Datasets/GP/GP Data on SAIL.doc
    #1 = NHS Number passes digit check test
    #4 = Surname, First Name, Post Code, Date of Birth and Gender match exactly to the AR
    #39 = Surname, Post Code, Date of Birth and Gender match exactly to the AR. First name
    #matches on Lexicon (known variants) or fuzzy matching probability >= 0.9
    gpEventsAllPop <- SAILDBUtils::runSQL(sail,qq("SELECT DISTINCT ALF_PE, GNDR_cD, WOB 
                                        FROM @{GP_PATIENT_DATABASE} 
                                        WHERE ALF_PE IS NOT NULL 
                                        AND ALF_STS_CD IN (1,4,39)"), strings_as_factors=FALSE)
    save(gpEventsAllPop,file=paste(WORKING_DIR,RAWSQL_DIR,"All Patients (Raw).RData",sep=""))
    
    SAILDBUtils::drop_table(sail,paste(PROJECT_WORK_NUMBER,".COHORT_ALF_PE"))
    SAILDBUtils::create_table_as(sail,paste(PROJECT_WORK_NUMBER,".COHORT_ALF_PE",sep=""),
                                 qq("SELECT ALF_PE,MIN(WOB) AS WOB 
                                    FROM @{GP_PATIENT_DATABASE} 
                                    WHERE ALF_PE IS NOT NULL 
                                    AND ALF_STS_CD IN (1,4,39) 
                                    GROUP BY ALF_PE"))
    
    recentLocString_LSOA <- paste(PROJECT_WORK_NUMBER,".MOST_RECENT_LOC_LSOA",sep="")
    recentLocString_RALF <- paste(PROJECT_WORK_NUMBER,".MOST_RECENT_LOC_RALF",sep="")
    SAILDBUtils::drop_table(sail,recentLocString_LSOA)
    SAILDBUtils::drop_table(sail,recentLocString_RALF)
    
    stillInWales <- SAILDBUtils::runSQL(sail,qq("SELECT DISTINCT ALF_PE,END_DATE
                                                FROM @{WDSD_WALES}
                                                WHERE (END_DATE='9999-01-01'
                                                OR END_DATE>='@{DATE_MAX}')
                                                AND START_DATE<='@{DATE_MAX}'
                                                AND WELSH_ADDRESS=1
                                                AND ALF_PE IS NOT NULL"),strings_as_factors=FALSE)
    
    save(stillInWales,file=paste(WORKING_DIR,RAWSQL_DIR,"WDSD People In Wales.RData",sep=""))
    
    #Getting most recent location for people on SAIL
    #Sorting by latest end date
    if(RALF_PRESENT==TRUE)
    {
      SAILDBUtils::create_table_as(sail,recentLocString_RALF,
                                   qq("SELECT *
                                    FROM(
	                                    SELECT ALF_PE,
		                                  START_DATE,
		                                  END_DATE,
		                                  RALF_PE,
		                                  LSOA2011_CD,
		                                  WELSH_ADDRESS,
		                                  ROW_NUMBER() OVER (PARTITION BY ALF_PE ORDER BY END_DATE DESC) AS DATE_RN
		                                  FROM @{WDSD_RALF_DATABASE} 
	                                    WHERE LSOA2011_CD IS NOT NULL
	                                    AND ALF_PE IS NOT NULL
	                                    AND START_DATE<='@{DATE_MAX}'
                                    )
                                    WHERE DATE_RN=1"))
      
      mostRecentLocation_RALF <- SAILDBUtils::runSQL(sail,qq("SELECT ALF_PE,
                                                          START_DATE AS WDSD_START_RALF,
                                                          END_DATE AS WDSD_END_RALF,
                                                          RALF_PE,
                                                          Loc.LSOA2011_CD AS LSOA2011_RALF,
                                                          LAD11CD AS LAD11_RALF,
                                                          WELSH_ADDRESS,
                                                          DATE_RN
                                                          FROM @{recentLocString_RALF} Loc
                                                        LEFT JOIN(
                                                          SELECT DISTINCT LSOA11CD AS LSOA2011_CD,LAD11CD 
                                                          FROM SAILUKHDV.ODS_LSOA01_LSOA11_LAD11_EW_LU_SCD
                                                          WHERE IS_LATEST=1 AND LSOA11CD LIKE 'W%'
                                                        )LAD ON Loc.LSOA2011_CD=LAD.LSOA2011_CD"),strings_as_factors = FALSE)
      save(mostRecentLocation_RALF,file=paste(WORKING_DIR,PREPROC_DIR,"Patient Most Recent Location (RALF).RData",sep=""))
    }

    SAILDBUtils::create_table_as(sail,paste(PROJECT_WORK_NUMBER,".MOST_RECENT_LOC_LSOA",sep=""),
                                 qq("SELECT *
                                    FROM(
	                                    SELECT ALF_PE,
		                                  START_DATE,
		                                  END_DATE,
		                                  LSOA2011_CD,
		                                  WELSH_ADDRESS,
		                                  ROW_NUMBER() OVER (PARTITION BY ALF_PE ORDER BY END_DATE DESC) AS DATE_RN
		                                  FROM @{WDSD_LOC_DATABASE} 
	                                    WHERE LSOA2011_CD IS NOT NULL
	                                    AND ALF_PE IS NOT NULL
	                                    AND START_DATE<='@{DATE_MAX}'
                                    )
                                    WHERE DATE_RN=1"))
      
    mostRecentLocation_LSOA <- SAILDBUtils::runSQL(sail,qq("SELECT ALF_PE,
                                                          START_DATE AS WDSD_START,
                                                          END_DATE AS WDSD_END,
                                                          Loc.LSOA2011_CD,
                                                          LAD11CD,
                                                          WELSH_ADDRESS,
                                                          DATE_RN
                                                          FROM @{recentLocString_LSOA} Loc
                                                        LEFT JOIN(
                                                          SELECT DISTINCT LSOA11CD AS LSOA2011_CD,LAD11CD 
                                                          FROM SAILUKHDV.ODS_LSOA01_LSOA11_LAD11_EW_LU_SCD
                                                          WHERE IS_LATEST=1 AND LSOA11CD LIKE 'W%'
                                                        )LAD ON Loc.LSOA2011_CD=LAD.LSOA2011_CD"),strings_as_factors = FALSE)
    
    save(mostRecentLocation_LSOA,file=paste(WORKING_DIR,PREPROC_DIR,"Patient Most Recent Location (LSOA).RData",sep=""))
    
    
    #Getting earliest entry in GP Database from the study period -1 year
    gpMinDateEvents <- SAILDBUtils::runSQL(sail, qq("SELECT ALF_PE, MIN(EVENT_DT) AS MIN_EVENT_DT
                                           FROM @{GP_EVENT_DATABASE}
                                           WHERE ALF_PE IS NOT NULL 
                                           AND ALF_STS_CD IN (1,4,39)
                                           AND EVENT_DT >= WOB
                                           AND EVENT_YR >= @{YEAR_MIN-1}
                                           AND EVENT_YR <= @{YEAR_MAX} 
                                           GROUP BY ALF_PE"),strings_as_factors=FALSE)
    save(gpMinDateEvents,file=paste(WORKING_DIR,PREPROC_DIR,"All Patients (Minimum Event Date).RData",sep=""))
    
    #Get the earliest entry in hospital data
    hospMinDate <- SAILDBUtils::runSQL(sail,qq("SELECT ALF_PE, MIN(admis_dt) AS MIN_EVENT_DT
                                               FROM @{PEDW_SPELL}
                                               WHERE ALF_PE IS NOT NULL
                                               GROUP BY ALF_PE"),strings_as_factors=FALSE)
    
    
    #Get the latest entry in hospital data
    hospMaxDate <- SAILDBUtils::runSQL(sail,qq("SELECT ALF_PE, MAX(admis_dt) AS MAX_EVENT_DT
                                               FROM @{PEDW_SPELL}
                                               WHERE ALF_PE IS NOT NULL
                                               GROUP BY ALF_PE"),strings_as_factors=FALSE)
    
    #Combine earliest and latest hosptial entries together
    hospRegPeriods <- hospMinDate %>%
      inner_join(hospMaxDate,by="ALF_PE")
    
    save(hospRegPeriods,file=paste(WORKING_DIR,PREPROC_DIR,"All Patients (Hosp Reg Periods).RData",sep=""))
  
    #Remove those with different dates of birth
    gpEventsDOB <- unique(gpEventsAllPop[,c("ALF_PE","WOB")])
    diffDatesOfBirth <- gpEventsDOB[duplicated(gpEventsDOB$ALF_PE),]
    gpSingleDOB <- gpEventsAllPop[!gpEventsAllPop$ALF_PE %in% diffDatesOfBirth$ALF_PE,]
  
    #Those who have different gender codes place on undetermined gender
    gpGender <- unique(gpSingleDOB[,c("ALF_PE","GNDR_CD")])
    diffGenders <- gpGender[duplicated(gpGender$ALF_PE),]
    gpSingleDOB[gpSingleDOB$ALF_PE %in% diffGenders$ALF_PE,c("GNDR_CD")] <- 3
  
    allPatients <- unique(gpSingleDOB)
    save(allPatients,file=paste(WORKING_DIR,PREPROC_DIR,"All Valid Patients.RData",sep=""))
    
    #Get General Demographics from Portal
    portalDemo <- SAILDBUtils::runSQL(sail,qq("SELECT 
    CASE WHEN ALF_STS_CD IN (1,4,39) THEN ALF_PE
      ELSE NULL
    END AS ALF_PE,
    SYSTEM_ID_PE AS USERID_PE,
    WOB,
    GNDR_CD,
    LSOA2001_CD
    FROM @{REGTOALF_DATABASE} Users"),strings_as_factors=FALSE)
    
    save(portalDemo,file=paste(WORKING_DIR,RAWSQL_DIR,"Portal Demographics (Raw).RData",sep=""))
  }
  else
  {
    #If we've already run the queries and aren't interested in re-running them, load the RData objects
    #where the query results are being held
    load(paste(WORKING_DIR,PREPROC_DIR,"All Patients (Minimum Event Date).RData",sep=""))
    load(paste(WORKING_DIR,PREPROC_DIR,"All Valid Patients.RData",sep=""))
    load(paste(WORKING_DIR,RAWSQL_DIR,"Portal Demographics (Raw).RData",sep=""))
    if(RALF_PRESENT==TRUE)
    {
      load(paste(WORKING_DIR,PREPROC_DIR,"Patient Most Recent Location (RALF).RData",sep=""))
    }
    
    load(paste(WORKING_DIR,PREPROC_DIR,"Patient Most Recent Location (LSOA).RData",sep=""))
  }
  
  if(DOWNLOAD_ALL_DEATHS==TRUE)
  {
    #Deaths from ADDE
    addeDeaths <- SAILDBUtils::runSQL(sail, qq("SELECT ALF_PE, DEATH_DT FROM 
                                      @{ADDE_DATABASE} 
                                      WHERE ALF_PE IS NOT NULL"),strings_as_factors=FALSE)
    save(addeDeaths,file=paste(WORKING_DIR,RAWSQL_DIR,"Deaths (ADDE).RData",sep=""))
    
    #Deaths from WDS
    wdsDeaths <- SAILDBUtils::runSQL(sail,qq("SELECT ALF_PE, DOD AS DEATH_DT FROM
                                      @{WDSD_DATABASE}
                                      WHERE ALF_PE IS NOT NULL
                                      AND DOD IS NOT NULL"))
    save(wdsDeaths,file=paste(WORKING_DIR,RAWSQL_DIR,"Deaths (WDS).RData",sep=""))
    
    #Try and find deaths listed in the WDS that aren't on ADDE
    wdsDeaths_DeathsNotListed <- wdsDeaths %>%
      filter(!ALF_PE %in% addeDeaths$ALF_PE)
    
    #Look for death codes in the GP Data
    gpDeathCodes <- SAILDBUtils::runSQL(sail,"SELECT READ_CD,READ_DESC
                                        FROM (SELECT READ_CD, READ_DESC FROM SAILREFRV.READ_CD
	                                      WHERE REGEXP_LIKE(READ_CD, '^[9]{1}[4]{1}[1-9]{1}.{2}$')
	                                      OR REGEXP_LIKE(READ_CD,'^[9]{1}[4]{1}[A-G]{1}.{2}$')
	                                      OR REGEXP_LIKE(READ_CD,'^[2]{1}[2]{1}[J]{1}.{2}$')
                                        UNION
                                        SELECT READ_CODE AS READ_CD, 
		                                    CASE WHEN PREF_TERM_60 IS NULL THEN PREF_TERM_30
		                                    ELSE PREF_TERM_60 END AS READ_DESC 
		                                    FROM SAILUKHDV.READ_CD_CV2_SCD
		                                    WHERE REGEXP_LIKE(READ_CODE, '^[9]{1}[4]{1}[1-9]{1}.{2}$')
                                        OR REGEXP_LIKE(READ_CODE, '^[9]{1}[4]{1}[A-G]{1}.{2}$')
                                        OR REGEXP_LIKE(READ_CODE, '^[2]{1}[2]{1}[J]{1}.{2}$')) Codes")
    
    deathCodesForQuery <- paste(as.character(gpDeathCodes$READ_CD),collapse="','")
    
    gpDeathData <- SAILDBUtils::runSQL(sail, qq("SELECT ALF_PE,EVENT_DT,
                                           EVENT_YR,EVENT_CD,EVENT_CD_VRS,EVENT_VAL,EPISODE,SEQUENCE 
                                           FROM @{GP_EVENT_DATABASE} 
                                           WHERE EVENT_CD IN ('@{deathCodesForQuery}') 
                                           AND ALF_PE IS NOT NULL
                                           AND EVENT_DT>=WOB
                                           AND EVENT_YR<=@{YEAR_MAX}"), strings_as_factors=FALSE)
    
    #Try and find deaths listed in GP Data not listed in WDS and ADDE
    gpDeathData_DeathsNotListed <- gpDeathData %>%
      filter(!ALF_PE %in% c(addeDeaths$ALF_PE,wdsDeaths_DeathsNotListed$ALF_PE))
    
    #Look for death codes in Hospital Data
    hospitalDeaths <- SAILDBUtils::runSQL(sail,qq("SELECT DISTINCT ALF_PE, DISCH_DT AS DOD
                                              FROM @{PEDW_SPELL}
                                              WHERE ALF_PE IS NOT NULL
                                              AND DISCH_MTHD_CD = 4
                                              AND DISCH_DT <= '@{DATE_MAX}'"), strings_as_factors=FALSE)
    
    #Try and find deaths listed in Hospital Data not listed in WDS,ADDE and GP Data
    hospitalDeaths_DeathsNotListed <- hospitalDeaths %>%
      filter(!ALF_PE %in% c(addeDeaths$ALF_PE,wdsDeaths_DeathsNotListed$ALF_PE,gpDeathData_DeathsNotListed$ALF_PE))
    
    save(gpDeathCodes,gpDeathData,file=paste(WORKING_DIR,RAWSQL_DIR,"Deaths (GP Data).RData",sep=""))
    save(addeDeaths,file=paste(WORKING_DIR,PREPROC_DIR,"Deaths (ADDE).RData",sep=""))
    save(wdsDeaths_DeathsNotListed,file=paste(WORKING_DIR,PREPROC_DIR,"Deaths (WDS).RData",sep=""))
    save(gpDeathCodes,gpDeathData_DeathsNotListed,file=paste(WORKING_DIR,PREPROC_DIR,"Deaths (GP Data).RData",sep=""))
    save(hospitalDeaths,file=paste(WORKING_DIR,RAWSQL_DIR,"Deaths (Hospital Data).RData",sep=""))
    save(hospitalDeaths_DeathsNotListed,file=paste(WORKING_DIR,PREPROC_DIR,"Deaths (Hospital Data).RData",sep=""))
  }
  else
  {
    #If we've already run the queries, load the RData objects which hold the query results
    load(paste(WORKING_DIR,PREPROC_DIR,"Deaths (ADDE).RData",sep=""))
    load(paste(WORKING_DIR,PREPROC_DIR,"Deaths (WDS).RData",sep=""))
    load(paste(WORKING_DIR,PREPROC_DIR,"Deaths (GP Data).RData",sep=""))
    load(paste(WORKING_DIR,PREPROC_DIR,"Deaths (Hospital Data).RData",sep=""))
  }
  
  #MS Events With Hospital Data
  msOnlyEvents <- SAILDBUtils::runSQL(sail, qq("SELECT DISTINCT ALF_PE,EVENT_DT,
        'GP' AS EVENT_TYPE
      FROM @{GP_EVENT_DATABASE}
      WHERE EVENT_CD='F20..'
      AND ALF_PE IS NOT NULL
      AND EVENT_YR>=@{YEAR_MIN}
      AND EVENT_YR<=@{YEAR_MAX}
      UNION ALL
      SELECT DISTINCT ALF_PE,
        admis_dt AS EVENT_DT,
        'HOSPITAL' AS EVENT_TYPE
      FROM @{PEDW_SPELL} p
      inner join @{PEDW_EPISODE} e 
      on (
        p.prov_unit_cd = e.prov_unit_cd
        and p.spell_num_pe = e.spell_num_pe
      ) 
      inner join @{PEDW_DIAG} d 
      on (
        e.prov_unit_cd = d.prov_unit_cd
        and e.spell_num_pe = d.spell_num_pe
        and e.epi_num = d.epi_num
      )
      WHERE d.diag_cd_1234 LIKE 'G35%'
      AND ALF_PE IS NOT NULL
      AND admis_dt>='@{DATE_MIN}'
      AND admis_dt<='@{DATE_MAX}'"),strings_as_factors=FALSE)
  
  msOnlyEvents_OPDW <- SAILDBUtils::runSQL(sail,qq("SELECT DISTINCT ALF_PE,
		 d.ATTEND_DT AS EVENT_DT,
		'HOSPITAL' AS EVENT_TYPE
	  FROM @{OPDW_DIAG} d
	  INNER JOIN @{OPDW_OUTPATIENT} p
	  ON(
		  d.PROV_UNIT_CD=p.PROV_UNIT_CD
		  AND d.AVAIL_FROM_DT=p.AVAIL_FROM_DT
		  AND d.ATT_ID_PE=p.ATT_ID_PE
		  AND d.CASE_REC_NUM_PE=p.CASE_REC_NUM_PE
		  AND d.ATTEND_DT=p.ATTEND_DT 
	  )
	  WHERE DIAG_CD_123='G35'
	  AND ALF_PE IS NOT NULL
	  AND d.ATTEND_DT>='@{DATE_MIN}'
	  AND d.ATTEND_DT<='@{DATE_MAX}'"),strings_as_factors=FALSE)
  
  save(msOnlyEvents,msOnlyEvents_OPDW,file=paste(WORKING_DIR,RAWSQL_DIR,"MS Only (Raw).RData",sep=""))
  
  msOnlyEvents_Clean <- preProcessEvents(msOnlyEvents,allPatients)
  msOnlyPatients <- unique(msOnlyEvents_Clean$ALF_PE)
  msOnlyEvents_OPDWClean <- preProcessEvents(msOnlyEvents_OPDW,allPatients)
  msOnlyPatients_OPDW <- unique(msOnlyEvents_OPDWClean$ALF_PE)
  save(msOnlyEvents_Clean,msOnlyPatients,msOnlyEvents_OPDWClean,msOnlyPatients_OPDW,file=paste(WORKING_DIR,PREPROC_DIR,"MS Only (Clean).RData",sep=""))

  #Extra Codes based on Manitoba Method to label patients as having potential MS
  #TODO: At some point we may need to revisit this to implement DMTs from GP Data, as these
  #are part of the Manitoba Method
  potentialMS <- SAILDBUtils::runSQL(sail, qq("SELECT DISTINCT ALF_PE,EVENT_DT,EVENT_CD,
        'GP' AS EVENT_TYPE
      FROM @{GP_EVENT_DATABASE}
      WHERE EVENT_CD IN ('F4H3.','F037.','F21yz','F21X.','F21y.')
      AND ALF_PE IS NOT NULL
      AND EVENT_YR>=@{YEAR_MIN}
      AND EVENT_YR<=@{YEAR_MAX}
      UNION ALL
      SELECT DISTINCT ALF_PE,
        admis_dt AS EVENT_DT,
        d.diag_cd_1234 AS EVENT_CD,
        'HOSPITAL' AS EVENT_TYPE
      FROM @{PEDW_SPELL} p
      inner join @{PEDW_EPISODE} e 
      on (
        p.prov_unit_cd = e.prov_unit_cd
        and p.spell_num_pe = e.spell_num_pe
      ) 
      inner join @{PEDW_DIAG} d 
      on (
        e.prov_unit_cd = d.prov_unit_cd
        and e.spell_num_pe = d.spell_num_pe
        and e.epi_num = d.epi_num
      )
      WHERE (d.diag_cd_123='H46'
      OR d.diag_cd_1234='G373'
      OR d.diag_cd_1234='G369'
      OR d.diag_cd_1234='G378'
      OR d.diag_cd_1234='G368')
      AND ALF_PE IS NOT NULL
      AND admis_dt>='@{DATE_MIN}'
      AND admis_dt<='@{DATE_MAX}'"),strings_as_factors=FALSE)
  
  potentialMS_OPDW <- SAILDBUtils::runSQL(sail,qq("SELECT DISTINCT ALF_PE,
		d.ATTEND_DT AS EVENT_DT,
		CASE WHEN DIAG_CD_4 IS NULL THEN DIAG_CD_123
		  ELSE CONCAT(DIAG_CD_123,DIAG_CD_4)
		END AS EVENT_CD,
		'HOSPITAL' AS EVENT_TYPE
	  FROM @{OPDW_DIAG} d
	  INNER JOIN @{OPDW_OUTPATIENT} p
	  ON(
		  d.PROV_UNIT_CD=p.PROV_UNIT_CD
		  AND d.AVAIL_FROM_DT=p.AVAIL_FROM_DT
		  AND d.ATT_ID_PE=p.ATT_ID_PE
		  AND d.CASE_REC_NUM_PE=p.CASE_REC_NUM_PE
		  AND d.ATTEND_DT=p.ATTEND_DT 
	  )
	  WHERE (DIAG_CD_123='H46'
	  OR (DIAG_CD_123='G37' AND DIAG_CD_4='3')
	  OR (DIAG_CD_123='G36' AND DIAG_CD_4='9')
	  OR (DIAG_CD_123='G37' AND DIAG_CD_4='8')
	  OR (DIAG_CD_123='G36' AND DIAG_CD_4='8'))
	  AND ALF_PE IS NOT NULL
	  AND d.ATTEND_DT>='@{DATE_MIN}'
	  AND d.ATTEND_DT<='@{DATE_MAX}'"),strings_as_factors=FALSE)
  
  save(potentialMS,potentialMS_OPDW,file=paste(WORKING_DIR,RAWSQL_DIR,"Potential MS (Raw).RData",sep=""))
  
  potentialMS_Clean <- preProcessEvents(potentialMS,allPatients)
  potentialMS_OPDWClean <- preProcessEvents(potentialMS_OPDW,allPatients)
  
  potentialMSPatients <- data.frame(ALF_PE=unique(c(potentialMS_Clean$ALF_PE,
                                                    potentialMS_OPDWClean$ALF_PE,
                                                    msOnlyPatients,
                                                    msOnlyPatients_OPDW)),stringsAsFactors = FALSE)
  
  SAILDBUtils::drop_table(sail,ALLMS_DATABASE)
  SAILDBUtils::create_table_from_df(sail,ALLMS_DATABASE,potentialMSPatients)
  
  save(potentialMS_Clean,potentialMS_OPDWClean,potentialMSPatients,file=paste(WORKING_DIR,PREPROC_DIR,"Potential MS (Clean).RData",sep=""))
  
  #Neuromyelitis Optica With Hospital Data
  neuromyeOptica <- SAILDBUtils::runSQL(sail, qq("SELECT DISTINCT ALF_PE,EVENT_DT,
        'GP' AS EVENT_TYPE
      FROM @{GP_EVENT_DATABASE}
      WHERE EVENT_CD='F210.'
      AND ALF_PE IS NOT NULL
      AND EVENT_YR>=@{YEAR_MIN}
      AND EVENT_YR<=@{YEAR_MAX}
      UNION ALL
      SELECT DISTINCT ALF_PE,
        admis_dt AS EVENT_DT,
        'HOSPITAL' AS EVENT_TYPE
      FROM @{PEDW_SPELL} p
      inner join @{PEDW_EPISODE} e 
      on (
        p.prov_unit_cd = e.prov_unit_cd
        and p.spell_num_pe = e.spell_num_pe
      ) 
      inner join @{PEDW_DIAG} d 
      on (
        e.prov_unit_cd = d.prov_unit_cd
        and e.spell_num_pe = d.spell_num_pe
        and e.epi_num = d.epi_num
      )
      WHERE d.diag_cd_1234='G360'
      AND ALF_PE IS NOT NULL
      AND admis_dt>='@{DATE_MIN}'
      AND admis_dt<='@{DATE_MAX}'"),strings_as_factors=FALSE)
  
  neuromyeOptica_OPDW <- SAILDBUtils::runSQL(sail,qq("SELECT DISTINCT ALF_PE,
		 d.ATTEND_DT AS EVENT_DT,
		'HOSPITAL' AS EVENT_TYPE
	  FROM @{OPDW_DIAG} d
	  INNER JOIN @{OPDW_OUTPATIENT} p
	  ON(
		  d.PROV_UNIT_CD=p.PROV_UNIT_CD
		  AND d.AVAIL_FROM_DT=p.AVAIL_FROM_DT
		  AND d.ATT_ID_PE=p.ATT_ID_PE
		  AND d.CASE_REC_NUM_PE=p.CASE_REC_NUM_PE
		  AND d.ATTEND_DT=p.ATTEND_DT 
	  )
	  WHERE DIAG_CD_123='G36' AND DIAG_CD_4='0'
	  AND ALF_PE IS NOT NULL
	  AND d.ATTEND_DT>='@{DATE_MIN}'
	  AND d.ATTEND_DT<='@{DATE_MAX}'"),strings_as_factors=FALSE)
  
  save(neuromyeOptica,neuromyeOptica_OPDW,file=paste(WORKING_DIR,RAWSQL_DIR,"NeuroOptica (Raw).RData",sep=""))
  
  neuromyeOptica_Clean <- preProcessEvents(neuromyeOptica,allPatients)
  neuromyeOptica_OPDWClean <- preProcessEvents(neuromyeOptica_OPDW,allPatients)
  save(neuromyeOptica_Clean,neuromyeOptica_OPDWClean,file=paste(WORKING_DIR,PREPROC_DIR,"NeuroOptica (Clean).RData",sep=""))
  
  #Family History of MS
  famHistoryMS <- getEvents("1292.",sail)
  save(famHistoryMS,file=paste(WORKING_DIR,RAWSQL_DIR,"Family History MS (Raw).RData",sep=""))
  famHistoryMS_Clean <- preProcessEvents(famHistoryMS,allPatients)
  famHistoryMSPatients <- unique(famHistoryMS_Clean$ALF_PE)
  save(famHistoryMS_Clean,famHistoryMSPatients,file=paste(WORKING_DIR,PREPROC_DIR,"Family History MS (Clean).RData",sep=""))
  
  #Close Connection
  SAILDBUtils::close_connection(sail)
}else
{
  load(paste(WORKING_DIR,PREPROC_DIR,"All Valid Patients.RData",sep=""))
  load(paste(WORKING_DIR,PREPROC_DIR,"Family History MS (Clean).RData",sep=""))
  load(paste(WORKING_DIR,PREPROC_DIR,"MS Only (Clean).RData",sep=""))
  load(paste(WORKING_DIR,PREPROC_DIR,"Generic Diabetes (Clean).RData",sep=""))
  load(paste(WORKING_DIR,PREPROC_DIR,"Deaths (ADDE).RData",sep=""))
  load(paste(WORKING_DIR,PREPROC_DIR,"Deaths (WDS).RData",sep=""))
  load(paste(WORKING_DIR,PREPROC_DIR,"Deaths (GP Data).RData",sep=""))
  load(paste(WORKING_DIR,PREPROC_DIR,"Deaths (Hospital Data).RData",sep=""))
  load(paste(WORKING_DIR,PREPROC_DIR,"All Patients (Minimum Event Date).RData",sep=""))
  load(paste(WORKING_DIR,PREPROC_DIR,"NeuroOptica (Clean).RData",sep=""))
  load(paste(WORKING_DIR,PREPROC_DIR,"Potential MS (Clean).RData",sep=""))
  if(RALF_PRESENT==TRUE)
  {
    load(paste(WORKING_DIR,PREPROC_DIR,"Patient Most Recent Location (RALF).RData",sep=""))
  }
  load(paste(WORKING_DIR,PREPROC_DIR,"Patient Most Recent Location (LSOA).RData",sep=""))
  load(paste(WORKING_DIR,RAWSQL_DIR,"WDSD People In Wales.RData",sep=""))
}

addeDeaths <- addeDeaths %>%
  mutate(DeathSource="ADDE")

wdsDeaths <- wdsDeaths_DeathsNotListed %>%
  mutate(DeathSource="WDS")

gpDeaths <- gpDeathData_DeathsNotListed %>%
  arrange(ALF_PE,EVENT_DT) %>%
  group_by(ALF_PE) %>%
  mutate(DeathAdminID=row_number()) %>%
  ungroup() %>%
  filter(DeathAdminID==1) %>%
  mutate(EVENT_DT=as.Date(EVENT_DT,format="%Y-%m-%d")) %>%
  rename(DEATH_DT=EVENT_DT) %>%
  mutate(DeathSource="GP")

hospitalDeaths <- hospitalDeaths_DeathsNotListed %>%
  arrange(ALF_PE,DOD) %>%
  group_by(ALF_PE) %>%
  mutate(DeathHospID=row_number()) %>%
  ungroup() %>%
  filter(DeathHospID==1) %>%
  rename(DEATH_DT=DOD) %>%
  mutate(DeathSource="PEDW")

allDeaths <- addeDeaths %>%
  rbind(wdsDeaths) %>%
  rbind(gpDeaths %>% select(ALF_PE,DEATH_DT,DeathSource)) %>%
  rbind(hospitalDeaths %>% select(ALF_PE,DEATH_DT,DeathSource)) %>%
  mutate(DeathDate=as.Date(ymd_hms(DEATH_DT)))

#####
msClassic <- msOnlyEvents_Clean

msOnlyEvents_Clean <- msOnlyEvents_Clean %>%
  rbind(msOnlyEvents_OPDWClean) %>%
  distinct()

#MS GP+Hosptial Combined (F20.. READ and G35 ICD10)
ms_ValidDates <- msOnlyEvents_Clean %>%
  arrange(ALF_PE,EVENT_DT) %>%
  rename(MSHospitalOnsetDate=EVENT_DT) %>%
  mutate(MSHospitalOnsetAge=as.numeric(round((MSHospitalOnsetDate-WOB)/365.25,digits=2))) %>%
  left_join(allDeaths %>% select(ALF_PE,DeathDate),by="ALF_PE") %>%
  mutate(AfterDeath=if_else(DeathDate<MSHospitalOnsetDate,1,0)) %>%
  filter((AfterDeath==0|is.na(AfterDeath))&MSHospitalOnsetAge>=0)

#Count up the MS Events (Needed for the cohort filters)
msEventCount <- ms_ValidDates %>%
  group_by(ALF_PE,EVENT_TYPE) %>%
  tally() %>%
  ungroup() %>%
  spread(EVENT_TYPE,n) %>%
  rename(MSGPEventCount=GP) %>%
  rename(MSHospitalEventCount=HOSPITAL) %>%
  mutate(MSGPEventCount=if_else(is.na(MSGPEventCount),0,as.numeric(MSGPEventCount))) %>%
  mutate(MSHospitalEventCount=if_else(is.na(MSHospitalEventCount),0,as.numeric(MSHospitalEventCount)))

potentialMSClassic <- potentialMS_Clean

potentialMS_Clean <- potentialMS_Clean %>%
  rbind(potentialMS_OPDWClean) %>%
  distinct()

#MS Manitoba Extra READ and ICD10 Codes
potentialMS_ValidDates <- potentialMS_Clean %>%
  select(-EVENT_CD) %>%
  distinct() %>%
  arrange(ALF_PE,EVENT_DT) %>%
  rename(PotentialMSOnsetDate=EVENT_DT) %>%
  mutate(PotentialMSOnsetAge=as.numeric(round((PotentialMSOnsetDate-WOB)/365.25,digits=2))) %>%
  left_join(allDeaths %>% select(ALF_PE,DeathDate),by="ALF_PE") %>%
  mutate(AfterDeath=if_else(DeathDate<PotentialMSOnsetDate,1,0)) %>%
  filter((AfterDeath==0|is.na(AfterDeath))&PotentialMSOnsetAge>=0)

msAllEvents <- ms_ValidDates %>%
  distinct() %>%
  arrange(ALF_PE,MSHospitalOnsetDate) %>%
  group_by(ALF_PE) %>%
  mutate(rowid=row_number()) %>%
  ungroup()

msTimeToThird <- msAllEvents %>%
  filter(rowid==3) %>%
  select(ALF_PE,MSHospitalOnsetDate) %>%
  rename(MSThirdEvent=MSHospitalOnsetDate)

msTimeToSeventh <- msAllEvents %>%
  filter(rowid==7) %>%
  select(ALF_PE,MSHospitalOnsetDate) %>%
  rename(MSSeventhEvent=MSHospitalOnsetDate)

#Optic Neuritis
opticNeuritis_ValidDates <- potentialMS_Clean %>%
  filter(EVENT_CD %in% c("H46X","F4H3.")) %>%
  mutate(OpticNOnsetDate=EVENT_DT) %>%
  mutate(OpticNOnsetAge=as.numeric(round((OpticNOnsetDate-WOB)/365.25,digits=2))) %>%
  left_join(allDeaths %>% select(ALF_PE,DeathDate),by="ALF_PE") %>%
  mutate(AfterDeath=if_else(DeathDate<OpticNOnsetDate,1,0)) %>%
  filter((AfterDeath==0|is.na(AfterDeath))&OpticNOnsetAge>=0)

opticNeuritis_EventCount <- opticNeuritis_ValidDates %>%
  group_by(ALF_PE,EVENT_TYPE) %>%
  tally() %>%
  ungroup() %>%
  spread(EVENT_TYPE,n) %>%
  rename(OpticNGPEventCount=GP) %>%
  rename(OpticNHospitalEventCount=HOSPITAL) %>%
  mutate(OpticNGPEventCount=if_else(is.na(OpticNGPEventCount),0,as.numeric(OpticNGPEventCount))) %>%
  mutate(OpticNHospitalEventCount=if_else(is.na(OpticNHospitalEventCount),0,as.numeric(OpticNHospitalEventCount)))

transMyelitis_ValidDates <- potentialMS_Clean %>%
  filter(EVENT_CD %in% c("G373","F037.")) %>%
  mutate(TransMyeOnsetDate=EVENT_DT) %>%
  mutate(TransMyeOnsetAge=as.numeric(round((TransMyeOnsetDate-WOB)/365.25,digits=2))) %>%
  left_join(allDeaths %>% select(ALF_PE,DeathDate),by="ALF_PE") %>%
  mutate(AfterDeath=if_else(DeathDate<TransMyeOnsetDate,1,0)) %>%
  filter((AfterDeath==0|is.na(AfterDeath))&TransMyeOnsetAge>=0)

transMyelitis_EventCount <- transMyelitis_ValidDates %>%
  group_by(ALF_PE,EVENT_TYPE) %>%
  tally() %>%
  ungroup() %>%
  spread(EVENT_TYPE,n) %>%
  rename(TransMyeGPEventCount=GP) %>%
  rename(TransMyeHospitalEventCount=HOSPITAL) %>%
  mutate(TransMyeGPEventCount=if_else(is.na(TransMyeGPEventCount),0,as.numeric(TransMyeGPEventCount))) %>%
  mutate(TransMyeHospitalEventCount=if_else(is.na(TransMyeHospitalEventCount),0,as.numeric(TransMyeHospitalEventCount)))

enceph_ValidDates <- potentialMS_Clean %>%
  filter(EVENT_CD %in% c("G369","F21X.")) %>%
  mutate(EncephOnsetDate=EVENT_DT) %>%
  mutate(EncephOnsetAge=as.numeric(round((EncephOnsetDate-WOB)/365.25,digits=2))) %>%
  left_join(allDeaths %>% select(ALF_PE,DeathDate),by="ALF_PE") %>%
  mutate(AfterDeath=if_else(DeathDate<EncephOnsetDate,1,0)) %>%
  filter((AfterDeath==0|is.na(AfterDeath))&EncephOnsetAge>=0)

enceph_EventCount <- enceph_ValidDates %>%
  group_by(ALF_PE,EVENT_TYPE) %>%
  tally() %>%
  ungroup() %>%
  spread(EVENT_TYPE,n) %>%
  rename(EncephGPEventCount=GP) %>%
  rename(EncephHospitalEventCount=HOSPITAL) %>%
  mutate(EncephGPEventCount=if_else(is.na(EncephGPEventCount),0,as.numeric(EncephGPEventCount))) %>%
  mutate(EncephHospitalEventCount=if_else(is.na(EncephHospitalEventCount),0,as.numeric(EncephHospitalEventCount)))

cnsDisease_ValidDates <- potentialMS_Clean %>%
  filter(EVENT_CD %in% c("G378","F21yz")) %>%
  mutate(CNSDisOnsetDate=EVENT_DT) %>%
  mutate(CNSDisOnsetAge=as.numeric(round((CNSDisOnsetDate-WOB)/365.25,digits=2))) %>%
  left_join(allDeaths %>% select(ALF_PE,DeathDate),by="ALF_PE") %>%
  mutate(AfterDeath=if_else(DeathDate<CNSDisOnsetDate,1,0)) %>%
  filter((AfterDeath==0|is.na(AfterDeath))&CNSDisOnsetAge>=0)

cnsDisease_EventCount <- cnsDisease_ValidDates %>%
  group_by(ALF_PE,EVENT_TYPE) %>%
  tally() %>%
  ungroup() %>%
  spread(EVENT_TYPE,n) %>%
  rename(CNSDisGPEventCount=GP) %>%
  rename(CNSDisHospitalEventCount=HOSPITAL) %>%
  mutate(CNSDisGPEventCount=if_else(is.na(CNSDisGPEventCount),0,as.numeric(CNSDisGPEventCount))) %>%
  mutate(CNSDisHospitalEventCount=if_else(is.na(CNSDisHospitalEventCount),0,as.numeric(CNSDisHospitalEventCount)))

accuteDemye_ValidDates <- potentialMS_Clean %>%
  filter(EVENT_CD %in% c("G368","F21y.")) %>%
  mutate(AccDemyeOnsetDate=EVENT_DT) %>%
  mutate(AccDemyeOnsetAge=as.numeric(round((AccDemyeOnsetDate-WOB)/365.25,digits=2))) %>%
  left_join(allDeaths %>% select(ALF_PE,DeathDate),by="ALF_PE") %>%
  mutate(AfterDeath=if_else(DeathDate<AccDemyeOnsetDate,1,0)) %>%
  filter((AfterDeath==0|is.na(AfterDeath))&AccDemyeOnsetAge>=0)

accuteDemye_EventCount <- accuteDemye_ValidDates %>%
  group_by(ALF_PE,EVENT_TYPE) %>%
  tally() %>%
  ungroup() %>%
  spread(EVENT_TYPE,n) %>%
  rename(AccDemyeGPEventCount=GP) %>%
  rename(AccDemyeHospitalEventCount=HOSPITAL) %>%
  mutate(AccDemyeGPEventCount=if_else(is.na(AccDemyeGPEventCount),0,as.numeric(AccDemyeGPEventCount))) %>%
  mutate(AccDemyeHospitalEventCount=if_else(is.na(AccDemyeHospitalEventCount),0,as.numeric(AccDemyeHospitalEventCount)))

#NMO (Used for Manitoba)
neuroClassic <- neuromyeOptica_Clean

neuromyeOptica_Clean <- neuromyeOptica_Clean %>%
  rbind(neuromyeOptica_OPDWClean) %>% 
  distinct()

neuromye_ValidDates <- neuromyeOptica_Clean %>%
  arrange(ALF_PE,EVENT_DT) %>%
  rename(NMOOnsetDate=EVENT_DT) %>%
  mutate(NMOOnsetAge=as.numeric(round((NMOOnsetDate-WOB)/365.25,digits=2))) %>%
  left_join(allDeaths %>% select(ALF_PE,DeathDate),by="ALF_PE") %>%
  mutate(AfterDeath=if_else(DeathDate<NMOOnsetDate,1,0)) %>%
  filter((AfterDeath==0|is.na(AfterDeath))&NMOOnsetAge>=0)

neuromyeOnset <- neuromye_ValidDates %>%
  group_by(ALF_PE) %>%
  mutate(rowid=row_number()) %>%
  ungroup() %>%
  filter(rowid==1) %>%
  select(ALF_PE,NMOOnsetDate,NMOOnsetAge)

#Count the NMO events
neuromye_EventCount <- neuromye_ValidDates %>%
  group_by(ALF_PE,EVENT_TYPE) %>%
  tally() %>%
  ungroup() %>%
  spread(EVENT_TYPE,n) %>%
  rename(NMOGPEventCount=GP) %>%
  rename(NMOHospitalEventCount=HOSPITAL) %>%
  mutate(NMOGPEventCount=if_else(is.na(NMOGPEventCount),0,as.numeric(NMOGPEventCount))) %>%
  mutate(NMOHospitalEventCount=if_else(is.na(NMOHospitalEventCount),0,as.numeric(NMOHospitalEventCount)))

neuromyeLatest <- neuromye_ValidDates %>%
  arrange(ALF_PE,desc(NMOOnsetDate)) %>%
  group_by(ALF_PE) %>%
  mutate(rowid=row_number()) %>%
  ungroup() %>%
  filter(rowid==1) %>%
  select(ALF_PE,NMOOnsetDate) %>%
  rename(NMOLatestDate=NMOOnsetDate)

#Combine the MS Code Frames with the Manitoba Extra Codes
ms_ValidDatesExtraCodes <- ms_ValidDates %>%
  rename(PotentialMSOnsetDate=MSHospitalOnsetDate) %>%
  rename(PotentialMSOnsetAge=MSHospitalOnsetAge) %>%
  mutate(PotentialMSEventOnset="MS") %>%
  rbind(potentialMS_ValidDates %>% 
          mutate(PotentialMSEventOnset="MSExtra")) %>%
  rbind(neuromye_ValidDates %>% 
          rename(PotentialMSOnsetDate=NMOOnsetDate) %>% 
          rename(PotentialMSOnsetAge=NMOOnsetAge) %>% 
          mutate(PotentialMSEventOnset="NMO")) %>%
  arrange(ALF_PE,PotentialMSOnsetDate,PotentialMSEventOnset)

msOnset <- ms_ValidDates %>%
  group_by(ALF_PE) %>%
  mutate(rowid=row_number()) %>%
  ungroup() %>%
  filter(rowid==1) %>%
  select(ALF_PE,MSHospitalOnsetDate,MSHospitalOnsetAge) %>%
  inner_join(msEventCount,by="ALF_PE")

potentialMSOnset <- ms_ValidDatesExtraCodes %>%
  group_by(ALF_PE) %>%
  mutate(rowid=row_number()) %>%
  ungroup() %>%
  filter(rowid==1) %>%
  select(ALF_PE,PotentialMSOnsetDate,PotentialMSOnsetAge)

#Family History of MS
fhmsOnset <- famHistoryMS_Clean %>%
  arrange(ALF_PE,EVENT_DT) %>%
  rename(FHMSOnsetDate=EVENT_DT) %>%
  mutate(FHMSOnsetAge=as.numeric(round((FHMSOnsetDate-WOB)/365.25,digits=2))) %>%
  left_join(allDeaths %>% select(ALF_PE,DeathDate),by="ALF_PE") %>%
  mutate(AfterDeath=if_else(DeathDate<FHMSOnsetDate,1,0)) %>%
  filter((AfterDeath==0|is.na(AfterDeath))&FHMSOnsetAge>=0) %>%
  group_by(ALF_PE) %>%
  mutate(rowid=row_number()) %>%
  ungroup() %>%
  filter(rowid==1) %>%
  select(ALF_PE,FHMSOnsetDate,FHMSOnsetAge)

#Generating All Patient Frame
allPatients_MasterFrame <- allPatients %>%
  mutate(YearOfBirth=year(WOB)) %>%
  inner_join(gpMinDateEvents,by="ALF_PE") %>%
  mutate(Gender=case_when(GNDR_CD==1 ~ 'Male',
                          GNDR_CD==2 ~ 'Female',
                          GNDR_CD==3 ~ 'Undetermined',
                          GNDR_CD==9 ~ 'Undetermined')) %>%
  left_join(msOnset,by="ALF_PE") %>%
  left_join(potentialMSOnset,by="ALF_PE") %>%
  left_join(neuromyeOnset,by="ALF_PE") %>%
  left_join(neuromyeLatest,by="ALF_PE") %>%
  left_join(msTimeToThird,by="ALF_PE") %>%
  left_join(msTimeToSeventh,by="ALF_PE") %>%
  left_join(fhmsOnset,by="ALF_PE") %>%
  left_join(allDeaths %>% select(ALF_PE,DeathDate,DeathSource),by="ALF_PE") %>%
  mutate(YearOfDeath=year(DeathDate)) %>%
  mutate(Dead=if_else(is.na(DeathDate),0,1)) %>%
  mutate(Dead=if_else(!is.na(DeathDate)&DeathDate>DATE_MAX,0,Dead)) %>%
  mutate(ExitDate=if_else(Dead==1,DeathDate,as.Date(DATE_MAX))) %>%
  mutate(AgeCurrent=as.numeric(round((ExitDate-WOB)/365.25,digits=2)))

healthBoard <- read_csv(paste(WORKING_DIR,"UA To Health Board.csv",sep=""))
allPatients_MasterFrame <- allPatients_MasterFrame %>%
  left_join(mostRecentLocation_LSOA %>% select(ALF_PE,LSOA2011_CD,LAD11CD,WDSD_START,WDSD_END),by="ALF_PE") %>%
  left_join(healthBoard %>% select(UA22CD,LHB22NM),by=c("LAD11CD"="UA22CD"))

if(RALF_PRESENT==TRUE)
{
  allPatients_MasterFrame <- allPatients_MasterFrame %>%
    left_join(mostRecentLocation_RALF %>% select(ALF_PE,RALF_PE,LSOA2011_RALF,LAD11_RALF,WDSD_START_RALF,WDSD_END_RALF),by="ALF_PE") %>%
    left_join(healthBoard %>% select(UA22CD,LHB22NM) %>% rename(LHB22NM_RALF=LHB22NM),by=c("LAD11_RALF"="UA22CD"))
}else
{
  allPatients_MasterFrame <- allPatients_MasterFrame %>%
    mutate(RALF_PE=NA_real_) %>%
    mutate(LSOA2011_RALF=NA_character_) %>%
    mutate(LAD11_RALF=NA_character_) %>%
    mutate(WDSD_START_RALF=as.Date(NA)) %>%
    mutate(WDSD_END_RALF=as.Date(NA)) %>%
    mutate(LHB22NM_RALF=NA_character_)
}

extraMSEvents_MasterFrame <- allPatients %>%
  inner_join(gpMinDateEvents,by="ALF_PE") %>%
  mutate(Gender=case_when(GNDR_CD==1 ~ 'Male',
                          GNDR_CD==2 ~ 'Female',
                          GNDR_CD==3 ~ 'Undetermined',
                          GNDR_CD==9 ~ 'Undetermined')) %>%
  left_join(msEventCount,by="ALF_PE") %>%
  left_join(opticNeuritis_EventCount,by="ALF_PE") %>%
  left_join(transMyelitis_EventCount,by="ALF_PE") %>%
  left_join(enceph_EventCount,by="ALF_PE") %>%
  left_join(cnsDisease_EventCount,by="ALF_PE") %>%
  left_join(accuteDemye_EventCount,by="ALF_PE") %>%
  left_join(neuromye_EventCount,by="ALF_PE") %>%
  replace(is.na(.),0)

save(extraMSEvents_MasterFrame,file=paste(WORKING_DIR,"MS Event Counts.RData",sep=""))
####

allPatients_MasterFrame <- allPatients_MasterFrame %>%
  filter(AgeCurrent>=0&YearOfBirth<2020)

save(allPatients_MasterFrame,file=paste(WORKING_DIR,"Patients Master Frame.RData",sep=""))
