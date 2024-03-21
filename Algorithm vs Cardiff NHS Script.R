library("SAILDBUtils")
library("GetoptLong")
library(tidyverse)
library(lubridate)
library(gtsummary)

#Main Project Working Directory
WORKING_DIR <<- "S:/0945 - UK MS Register/msincidence (JNNP Final)/"
source(paste(WORKING_DIR,"Define File Constants.R",sep=""))

#Load the whole population
load(paste(WORKING_DIR,COHORT_DIR,"Clean Population Table (V1 Alive).RData",sep=""))
#Load the UKMSR Cohort
load(paste(WORKING_DIR,COHORT_DIR,"MS Cohort (V2 - UKMSR) Alive.RData",sep=""))

#Get the Cardiff NHS Cohort Data
sail <- SAILDBUtils::SAILConnect()

#Initial Query just filtered diagnosis before or equal to Dec 31st 2020
cardiffNHSCohort_All <- SAILDBUtils::runSQL(sail,qq("SELECT DISTINCT ALF_PE,
                                            WOB,
                                            Cardiff.LSOA2011_CD,
                                            LAD11CD,
                                            GNDR_CD,
                                            DIAGNOSIS,
                                            DATE_OF_DIAGNOSIS,
                                            DATE_OF_ONSET
                                          FROM SAIL0945V.UKMS_MSREGISTER_ALF_20230210 Cardiff
                                          INNER JOIN(
                                            SELECT SYSTEM_ID_PE,
                                              DIAGNOSIS,
                                              DATE_OF_DIAGNOSIS AS DATE_OF_DIAGNOSIS,
                                              DATE_OF_ONSET
                                              FROM SAIL0945V.UKMS_MSTYPE_20230210
                                          )CardiffDetails ON Cardiff.SYSTEM_ID_PE=CardiffDetails.SYSTEM_ID_PE
                                          LEFT JOIN(
                                              SELECT DISTINCT LSOA11CD AS LSOA2011_CD,LAD11CD 
                                              FROM SAILUKHDV.ODS_LSOA01_LSOA11_LAD11_EW_LU_SCD
                                              WHERE IS_LATEST=1 AND LSOA11CD LIKE 'W%'
                                          )LAD ON Cardiff.LSOA2011_CD=LAD.LSOA2011_CD"),strings_as_factors=FALSE) %>%
  #Some additional pre-processing as some users have multiple diagnosis dates
  arrange(ALF_PE,DATE_OF_DIAGNOSIS) %>%
  group_by(ALF_PE) %>%
  mutate(rowid=row_number()) %>%
  ungroup() %>%
  filter(rowid==1)

msEventsHosp <- SAILDBUtils::runSQL(sail,"SELECT DISTINCT ALF_PE,
		p.PROV_UNIT_CD,
		ORG_NAME,
    admis_dt AS EVENT_DT
	  FROM SAIL0945V.PEDW_SPELL_20210328 p
      inner join SAIL0945V.PEDW_EPISODE_20210328 e 
      on (
        p.prov_unit_cd = e.prov_unit_cd
        and p.spell_num_pe = e.spell_num_pe
      ) 
      inner join SAIL0945V.PEDW_DIAG_20210328 d 
      on (
        e.prov_unit_cd = d.prov_unit_cd
        and e.spell_num_pe = d.spell_num_pe
        and e.epi_num = d.epi_num
      )
      LEFT JOIN SAILREFRV.PROV_UNIT_CD refrv ON p.PROV_UNIT_CD=refrv.PROV_UNIT_CD
      WHERE d.diag_cd_1234 LIKE 'G35%'
      AND ALF_PE IS NOT NULL
      AND admis_dt>='1970-01-01'
      AND admis_dt<='2020-12-31'",strings_as_factors=FALSE)

#Per Cardiff's request, now filtering to 1st January 2020 on diagnosis
cardiffNHSCohort_MSOnly <- cardiffNHSCohort_All %>%
  filter(DIAGNOSIS=="MS") %>%
  select(-LAD11CD,-LSOA2011_CD) %>%
  mutate(AgeDiag_FromDataset=round(as.numeric(DATE_OF_DIAGNOSIS-WOB)/365.25,digits=2)) %>%
  mutate(AgeOnset_FromDataset=round(as.numeric(DATE_OF_ONSET-WOB)/365.25,digits=2))
  
cardiffNHSCohort_Filtered <- cardiffNHSCohort_MSOnly %>%
  filter(DATE_OF_DIAGNOSIS<=as.Date('2020-01-01',"%Y-%m-%d")&DATE_OF_DIAGNOSIS>=as.Date('1970-01-01',"%Y-%m-%d"))

cardiffNHSCohort_CleanPop <- cleanPopulationTable_Alive %>%
  inner_join(cardiffNHSCohort_Filtered,by="ALF_PE")

cardiffNHSCohort_CleanPopInCardiff <- cardiffNHSCohort_CleanPop %>%
  filter(LHB22NM=="Cardiff and Vale University Health Board")

cardiffNHSCohort_NoHospitalEntryListed <- cardiffNHSCohort_CleanPopInCardiff %>%
  filter(MSHospitalEventCount==0) %>%
  mutate(Group="Cardiff NHS No Hospital Listed")

#Filtering the diagnosis date to 1st Jan 2020 to keep it similar to Cardiff
msCohortAlive_UKMSRFiltered <- msCohortAlive_UKMSR %>%
  filter(PotentialMSOnsetDate<='2020-01-01')

msCohortAlive_UKMSRInCardiff <- msCohortAlive_UKMSRFiltered %>%
  filter(LHB22NM=="Cardiff and Vale University Health Board")

msCohortAlive_MatchedToCardiffNHS <- msCohortAlive_UKMSRInCardiff %>%
  filter(ALF_PE %in% cardiffNHSCohort_CleanPopInCardiff$ALF_PE)

msCohort_MissingInCardiffCohort <- msCohortAlive_UKMSRInCardiff %>%
  filter(!ALF_PE %in% cardiffNHSCohort_CleanPopInCardiff$ALF_PE) %>%
  mutate(Group="UKMSR Cardiff Not Matched To Cardiff NHS") %>%
  mutate(AgeOnsetCardiff=NA_real_) %>%
  mutate(AgeDiagCardiff=NA_real_)

#####

cohortImportForSiteCheck <- msCohort_MissingInCardiffCohort %>%
  select(ALF_PE,Group) %>%
  rbind(cardiffNHSCohort_NoHospitalEntryListed %>% select(ALF_PE,Group))

importForDatabase <- as.data.frame(cohortImportForSiteCheck)

importResult <- SAILDBUtils::create_table_from_df(sail,"SAILW0945V.CARDIFF_SITE_CHECK_MARCH2024",importForDatabase)

#Finding out who has had admissions in PEDW in general
#Note: This is checking PEDW in 2022-07-04 view. View used on project was 2021-03-28
pedwSiteCheck <- SAILDBUtils::runSQL(sail,"SELECT table1.*,cd.GROUP FROM(
	SELECT DISTINCT ALF_PE,ps.PROV_UNIT_CD,ORG_NAME
	FROM SAIL0945V.PEDW_SPELL_20220704 ps 
	LEFT JOIN SAILREFRV.PROV_UNIT_CD refrv ON ps.PROV_UNIT_CD=refrv.PROV_UNIT_CD
	WHERE ALF_PE IN (SELECT ALF_PE FROM SAILW0945V.CARDIFF_SITE_CHECK_MARCH2024)
)table1
INNER JOIN(SELECT ALF_PE,GROUP FROM SAILW0945V.CARDIFF_SITE_CHECK_MARCH2024)cd ON table1.ALF_PE=cd.ALF_PE",strings_as_factors=FALSE)

SAILDBUtils::close_connection(sail)

pedwSiteCheck2 <- pedwSiteCheck %>%
  mutate(InCardiff=if_else(PROV_UNIT_CD %in% c("7A4","RWM"),1,0,missing=0))

#Trying to find out which people have been logged in a Cardiff site
pedwSiteCheck3 <- pedwSiteCheck2 %>%
  arrange(ALF_PE,GROUP,desc(InCardiff)) %>%
  select(ALF_PE,GROUP,InCardiff) %>%
  group_by(ALF_PE) %>%
  mutate(rowid=row_number()) %>%
  ungroup() %>%
  filter(rowid==1)

#Quick tally between the Cardiff NHS group and the UKMSR Not Cardiff group
pedwSiteCheckTally <- pedwSiteCheck3 %>%
  group_by(GROUP,InCardiff) %>%
  tally()

#####

msCohort_PresentInMainCardiffSet <- cardiffNHSCohort_All %>%
  filter(ALF_PE %in% msCohort_MissingInCardiffCohort$ALF_PE)

msCohort_MissingInCardiffCohort2 <- msCohort_MissingInCardiffCohort %>%
  filter(!ALF_PE %in% cardiffNHSCohort_All$ALF_PE)

msCohort_MissingInCardiff_GPOnly <- msCohort_MissingInCardiffCohort2 %>%
  filter(MSHospitalEventCount==0)

gpOnlyDemoStats <- msCohort_MissingInCardiff_GPOnly %>%
  summarise(Female=sum(Gender=="Female"),
            MeanAgeCurrent=round(mean(AgeCurrent),digits=1),
            SDAgeCurrent=round(sd(AgeCurrent),digits=1),
            MeanAgeDiag=round(mean(CohortDiagAge),digits=1),
            SDAgeDiag=round(sd(CohortDiagAge),digits=1),
            MeanGPEventCount=round(mean(MSGPEventCount),digits=1),
            SDGPEventCount=round(sd(MSGPEventCount),digits=1))

#Finding MS events in hospital for those still on the books
msCohort_CardiffMSEvents <- msEventsHosp %>% 
  inner_join(msCohort_MissingInCardiffCohort2,by="ALF_PE") %>% 
  mutate(InCardiff=if_else(PROV_UNIT_CD %in% c("7A4","RWM"),1,0,missing=0))

#Finding out who is in cardiff and who isn't
msCohort_CardiffHospital <- msCohort_CardiffMSEvents %>%
  filter(InCardiff==1) %>%
  select(ALF_PE,EVENT_DT) %>%
  arrange(ALF_PE,desc(EVENT_DT)) %>%
  group_by(ALF_PE) %>%
  mutate(rowid=row_number()) %>%
  ungroup() %>%
  filter(rowid==1)

msCohort_NotCardiffHospital <- msCohort_CardiffMSEvents %>%
  filter(InCardiff==0&!ALF_PE %in% msCohort_CardiffHospital$ALF_PE) %>%
  select(ALF_PE,EVENT_DT) %>%
  arrange(ALF_PE,desc(EVENT_DT)) %>%
  group_by(ALF_PE) %>%
  mutate(rowid=row_number()) %>%
  ungroup() %>%
  filter(rowid==1)

ukmsrInCardiffAndPEDW <- pedwSiteCheck3 %>%
  select(-rowid) %>%
  filter(GROUP=="UKMSR Cardiff Not Matched To Cardiff NHS") %>%
  left_join(msCohort_CardiffHospital %>% select(ALF_PE,EVENT_DT) %>% rename(CardiffDate=EVENT_DT),by="ALF_PE") %>%
  left_join(msCohort_NotCardiffHospital %>% select(ALF_PE,EVENT_DT) %>% rename(NotCardiffDate=EVENT_DT),by="ALF_PE")

peopleWithDates <- ukmsrInCardiffAndPEDW %>%
  filter(!is.na(CardiffDate)|!is.na(NotCardiffDate)) %>%
  inner_join(cleanPopulationTable_Alive,by="ALF_PE")

demo_Post2015 <- peopleWithDates %>%
  filter(year(CardiffDate)>=2015) %>%
  summarise(Female=sum(Gender=="Female"),
            MeanAgeCurrent=round(mean(AgeCurrent),digits=1),
            SDAgeCurrent=round(sd(AgeCurrent),digits=1),
            MeanAgeDiag=round(mean(PotentialMSOnsetAge),digits=1),
            SDAgeDiag=round(sd(PotentialMSOnsetAge),digits=1),
            MeanGPEventCount=round(mean(MSGPEventCount),digits=1),
            SDGPEventCount=round(sd(MSGPEventCount),digits=1),
            MeanHospEventCount=round(mean(MSHospitalEventCount),digits=1),
            SDHospEventCount=round(sd(MSHospitalEventCount),digits=1))

####
#The steps on slide 4 which lead to the xxx cohort!
msCohortCardiffVale_Final <- msCohortAlive_UKMSR %>% 
  filter(LHB22NM=="Cardiff and Vale University Health Board"&PotentialMSOnsetDate<='2020-01-01')
msCohortCardiffVale_Final2 <- msCohortCardiffVale_Final %>% 
  filter(!ALF_PE %in% msCohort_PresentInMainCardiffSet$ALF_PE)
msCohortCardiffVale_Final3 <- msCohortCardiffVale_Final2 %>% 
  filter(!ALF_PE %in% msCohort_MissingInCardiff_GPOnly$ALF_PE)
msCohortCardiffVale_Final4 <- msCohortCardiffVale_Final3 %>% 
  filter(!ALF_PE %in% msCohort_NotCardiffHospital$ALF_PE)

msCohortCardiffVale_Final4NotInCardiff <- msCohortCardiffVale_Final4 %>%
  filter(!ALF_PE %in% cardiffNHSCohort_CleanPop$ALF_PE)

#The pre-processing steps we did for the slides on Cardiff
cardiffNHSCohort_Final <- cardiffNHSCohort_All %>%
  filter(DIAGNOSIS=="MS"&DATE_OF_DIAGNOSIS>="1970-01-01"&DATE_OF_DIAGNOSIS<="2020-01-01") %>%
  select(ALF_PE,DIAGNOSIS,DATE_OF_DIAGNOSIS) %>%
  inner_join(cleanPopulationTable_Alive %>% select(ALF_PE,LHB22NM),by="ALF_PE")

#Based on the Location recorded by Cardiff. 
cardiffNHSCohort_Final2 <- cardiffNHSCohort_Final %>%
  filter(LHB22NM=="Cardiff and Vale University Health Board")

#Matching Cardiff to Cardiff
cardiffNHSCohort_Final3 <- cardiffNHSCohort_Final2 %>%
  filter(ALF_PE %in% msCohortAlive_UKMSRInCardiff$ALF_PE)

cardiffNHSCohort_FinalMissing <- cardiffNHSCohort_Final2 %>%
  filter(!ALF_PE %in% msCohortAlive_UKMSRInCardiff$ALF_PE) %>%
  inner_join(cleanPopulationTable_Alive,by="ALF_PE")
####
#Numbers for Confusion Matrix
peopleLocatedInCardiff <- cleanPopulationTable_Alive %>%
  filter(LHB22NM=="Cardiff and Vale University Health Board")

remainderOfPopInCardiff_MinusUKMSR <- peopleLocatedInCardiff %>%
  filter(!ALF_PE %in% msCohortCardiffVale_Final2$ALF_PE)

remainderOfPopInCardiff_MinusNHSCardiff <- peopleLocatedInCardiff %>%
  filter(!ALF_PE %in% cardiffNHSCohort_Final2$ALF_PE)

testPop1 <- remainderOfPopInCardiff_MinusUKMSR %>%
  filter(!ALF_PE %in% cardiffNHSCohort_FinalMissing$ALF_PE)

testPop2 <- remainderOfPopInCardiff_MinusNHSCardiff %>%
  filter(!ALF_PE %in% msCohort_MissingInCardiffCohort2$ALF_PE)

all.equal(testPop1,testPop2)

####
#Testing out the register portal people, finding out how many come back to cardiff

load(paste(WORKING_DIR,OUT_DIR_ALIVE,"UKMSR Matches.RData",sep=""))

matchDemoTable_RegisterMatched <- ukmsrResults_All %>%
  filter(AllGroup=="UKMSR - Disease Present and Tested Positive") %>%
  rename(AgeOnset_FromDataset=AgeOnsetMSRegister,
         AgeDiag_FromDataset=AgeDiagMSRegister) %>%
  select(ALF_PE,AgeCurrent,PotentialMSOnsetAge,AgeOnset_FromDataset,AgeDiag_FromDataset) %>%
  mutate(OnsetToDiag_FromDataset=AgeDiag_FromDataset-AgeOnset_FromDataset) %>%
  mutate(Group="UK MS Register Population (Matched)")

matchDemoTable_RegisterAll <- ukmsrResults_All %>%
  rename(AgeOnset_FromDataset=AgeOnsetMSRegister,
         AgeDiag_FromDataset=AgeDiagMSRegister) %>%
  mutate(PotentialMSOnsetAge=if_else(UKMSRPatient==0,NA_real_,PotentialMSOnsetAge)) %>%
  select(ALF_PE,AgeCurrent,PotentialMSOnsetAge,AgeOnset_FromDataset,AgeDiag_FromDataset) %>%
  mutate(OnsetToDiag_FromDataset=AgeDiag_FromDataset-AgeOnset_FromDataset) %>%
  mutate(Group="UK MS Register Population (All)")

matchDemoTable_CardiffAll <- cardiffNHSCohort_CleanPop %>%
  mutate(PotentialMSOnsetAge=if_else(UKMSRPatient==0,NA_real_,PotentialMSOnsetAge)) %>%
  select(ALF_PE,AgeCurrent,PotentialMSOnsetAge,AgeOnset_FromDataset,AgeDiag_FromDataset) %>%
  mutate(OnsetToDiag_FromDataset=AgeDiag_FromDataset-AgeOnset_FromDataset) %>%
  mutate(Group="Cardiff NHS Cohort (All)")

matchDemoTable_CardiffMatched <- cardiffNHSCohort_CleanPop %>%
  filter(ALF_PE %in% msCohortAlive_UKMSR$ALF_PE) %>%
  select(ALF_PE,AgeCurrent,PotentialMSOnsetAge,AgeOnset_FromDataset,AgeDiag_FromDataset) %>%
  mutate(OnsetToDiag_FromDataset=AgeDiag_FromDataset-AgeOnset_FromDataset) %>%
  mutate(Group="Cardiff NHS Cohort (Matched)")

matchDemoTable_Combined <- matchDemoTable_RegisterMatched %>%
  rbind(matchDemoTable_CardiffMatched) %>%
  rbind(matchDemoTable_CardiffAll) %>%
  rbind(matchDemoTable_RegisterAll) %>%
  mutate(Group=factor(Group,levels=c("UK MS Register Population (All)","Cardiff NHS Cohort (All)","UK MS Register Population (Matched)","Cardiff NHS Cohort (Matched)")))

matchDataTableComparison <- matchDemoTable_Combined %>%
  mutate(AgeCurrent=as.numeric(AgeCurrent)) %>%
  select(-ALF_PE) %>%
  tbl_summary(by=Group,
              type = list(all_continuous() ~ "continuous"),
              digits = list(all_continuous() ~ c(1,1,0)),
              statistic = list(all_continuous() ~ "{mean} ({sd}), {N_nonmiss}"),
              missing="no")

matchDataTableOutput <- matchDataTableComparison %>%
  as_tibble()

write.csv(matchDataTableOutput,file=paste(WORKING_DIR,TESTOUT_DIR_ALIVE,"Matches Stats Table.csv",sep=""),row.names=FALSE)
