

%let LReseau = ~/NAS/X ; 
%LET Arrete = 2026_04_V2;
%LET Arrete3 = 2026_04_V2; 
%let Ouput=CR_Q125;
%let month=03;
%let day=27;
%let yr=2026;
%let tx_claims_handling = 0.03;  /* A chaque Closing demander le taux Claims Handling.  Envoyé par  HORGAN Alan <alan.horgan@partners.axa>  ou BARRY David <david.barry@partners.axa> */ 

LIBNAME &Ouput. "&LReseau./08.Progammes/INTERNATIONAL/06_Inventaire CLP/&Arrete./02_Elements_Techniques/TIA/Arrete reel/RESERVES/ON-SYSTEM/CASES RESERVES/Output";
LIBNAME TIA "&LReseau./08.Progammes/INTERNATIONAL/06_Inventaire CLP/&Arrete3./02_Elements_Techniques/TIA/Extraction Donnees/TIA";


%let Import_02="&LReseau./08.Progammes/Etablissements financiers/PROJETS TRANSVERSAUX/Genworth/TOM Reserving/Table de reserving/Donnees/FY16/Data EXCEL/R/PROCESS RESERVING CLP/ON-SYSTEM/CASE RESERVES/02 Model Properties/Entity_Mappings.xlsx";
%MACRO IMPORT_EXCEL(FILE=,OUT=,ONGLET=,);
PROC IMPORT                 
DATAFILE= &FILE.
OUT= WORK.&OUT. 
DBMS=xlsx REPLACE;
SHEET=&ONGLET. ;
RUN;
%MEND;

%IMPORT_EXCEL(FILE=&Import_02.,OUT=Entity_Mappings,ONGLET=Entity_Mappings);
%IMPORT_EXCEL(FILE=&Import_02.,OUT=RI_Inwards,ONGLET=RI_Inwards);
%IMPORT_EXCEL(FILE=&Import_02.,OUT=RI_Inwards2,ONGLET=RI_Inwards2);
%IMPORT_EXCEL(FILE=&Import_02.,OUT=CI,ONGLET=CI);
%IMPORT_EXCEL(FILE=&Import_02.,OUT=RI_Ceded,ONGLET=RI_Ceded);
%IMPORT_EXCEL(FILE=&Import_02.,OUT=FX,ONGLET=FX);


%Macro ACR(pays=,);

/*#################################################################################################################################################################################
/*######################################################    CASE RESERVES :    ##############################################
/*################################################################################################################################################################################ */

/* %LET pays= FI ; */ 

DATA CR_&pays.;
retain;
keep country Rsrv_Grp Scheme Type_Insurance Clm_Nmbr cover Vintage_year Entity_CD Entity Quarter Rsrv_Amt_Net POSTE Acc_yr Acc_Mnth Entity_CD2   ;
set &Ouput..WPS_DAAP_CASE_RESERVES_&yr.&month.&day.;
where country="&pays." and STATUS  in ("OP","RO") and Rsrv_Typ in ("ICOP","RBNP") and Rsrv_Grp NOT in ("ZZ1","ZZ2")  AND LEGACY_SCHEME_BOOK="TIA"; 
Rename 
Vintage_year=Cohort
Rsrv_Amt_Net=Rsrv_Amt;
Acc_yr=year(Incident_date);
Acc_Mnth=MONTH(Incident_date);
if Acc_Mnth in (1,2,3)    THEN Quarter=cats(Acc_Yr,"Q1");
if Acc_Mnth in (4,5,6)    THEN Quarter=cats(Acc_Yr,"Q2");
if Acc_Mnth in (7,8,9)    THEN Quarter=cats(Acc_Yr,"Q3");
if Acc_Mnth in (10,11,12) THEN Quarter=cats(Acc_Yr,"Q4");
IF Rsrv_Typ in ("ICOP") THEN POSTE="ICOP_CLOT" ;
IF Rsrv_Typ in ("RBNP") THEN POSTE="RBNP_CLOT" ;
Entity_CD2=Entity_CD*1 ;
run;


PROC SQL;
      create table CR_&pays. as
             SELECT country,
                    Rsrv_Grp,
                    Scheme,
                    Type_Insurance,
                    Cohort,
                    cover,
                    Entity_CD2 as Entity_CD,
                    Entity,
                    Quarter,
                    POSTE,
                    sum(Rsrv_Amt) as Rsrv_Amt                                     
     FROM    CR_&pays.
     group by country, Rsrv_Grp,Scheme,Type_Insurance, cover,Cohort,Entity_CD2,Entity, Quarter,POSTE
      ; 
 quit;



/*#################################################################################################################################################################################
/*######################################################    IBNR & NON-CORE :     ##############################################
/*################################################################################################################################################################################ */

DATA IBNR_&pays.;
retain;
keep country Rsrv_Grp Scheme Type_Insurance Clm_Nmbr cover Vintage_year Entity_CD Entity Incident_Quarter Rsrv_Amt_Net POSTE Entity_CD2  ;
set &Ouput..WPS_DAAP_IBNR_&yr.&month.&day.;
where country="&pays." ;
Rename 
Vintage_year=Cohort
Rsrv_Amt_Net=Rsrv_Amt
Incident_Quarter=Quarter
;
POSTE="IBNR_CLOT" ;
Entity_CD2=Entity_CD*1 ;
run;


PROC SQL;
      create table IBNR_&pays. as
             SELECT country,
                    Rsrv_Grp,
                    Scheme,
                    Type_Insurance,
                    Cohort,
                    cover,
                    Entity_CD2 as Entity_CD,
                    Entity,
                    Quarter,
                    POSTE,
                    sum(Rsrv_Amt) as Rsrv_Amt                                      
     FROM    IBNR_&pays.
     group by country, Scheme,Type_Insurance,Rsrv_Grp, cover,Cohort,Entity_CD,Entity,Quarter,POSTE
      ; 
 quit;
 
DATA RESERVES_TIA_&pays. ;
set CR_&pays. IBNR_&pays.   ;
run;
 


proc sql ; 
create table RESERVES_TIA_&pays._1 as
select DISTINCT
t1.*,
t8.Currency AS Currency_code
from RESERVES_TIA_&pays. /*_ALL*/ t1 
left join FX t8 on (t1.country=t8.country )     
;
quit ; 

data RESERVES_TIA_&pays._2;
Set RESERVES_TIA_&pays._1 ;
run;


data RESERVES_TIA_&pays._3;
set RESERVES_TIA_&pays._2;
Total_AXA=Rsrv_Amt;
Claims_Handling=Total_AXA*&tx_claims_handling.;
PAD=(Total_AXA + Claims_Handling)*0.05;
NET_AMT= Total_AXA + Claims_Handling ;
run;

data STOCK_ACR_TIA_&pays.;
set RESERVES_TIA_&pays._3 ;
if Entity_CD in (102,122,132142,152,172,192,212,302,312,502,682,702,712,772,782,792,802,812,821,822,832,842,851,852,861,862,872,882,892,902,912,922,931,932,942,951,952,961,962,972,982,992) then Entity = "FACL"; 
else if Entity_CD in (101,121,131,141,151,171,301,311,501,671,681,701,711,771,791,801,811,821,831,841,851,861,871,881,891,901,911,921,931,941,951,961,971,981,991) then Entity ="FICL"; 
else Entity ="UNKNOWN";
run;

PROC SQL;
	CREATE TABLE STOCK_ACR_TIA_&pays. as 
	SELECT DISTINCT 
			h.*,
			s.partner_sales_name AS Agent
			FROM STOCK_ACR_TIA_&pays. h
	left JOIN TIA.CARTO_TIA s ON (h.Country=s.Country  and h.scheme=s.scheme)
	
	;
QUIT;



/*#################################################################################################################################################################################
/*######################################################   CLAIMS-PAID   ##############################################
/*################################################################################################################################################################################ */

Data CLAIM_PAID_&pays.;
keep country Rsrv_Grp Scheme Type_Insurance Clm_Nmbr cover Vintage_year Entity_CD Entity Quarter Totl_Amnt_Pd_Net Entity_CD2 ;
set &Ouput..WPS_DAAP_CASE_RESERVES_&yr.&month.&day.;
where country="&pays." ; 
Rename 
Vintage_year=Cohort
Totl_Amnt_Pd_Net=Totl_Amnt_Pd
;
Acc_yr=year(Incident_date);
Acc_Mnth=Month(Incident_date);
if Acc_Mnth in (1,2,3)    THEN Quarter=cats(Acc_Yr,"Q1");
if Acc_Mnth in (4,5,6)    THEN Quarter=cats(Acc_Yr,"Q2");
if Acc_Mnth in (7,8,9)    THEN Quarter=cats(Acc_Yr,"Q3");
if Acc_Mnth in (10,11,12) THEN Quarter=cats(Acc_Yr,"Q4");
Entity_CD2=Entity_CD*1 ;
run;


PROC SQL;
      create table CLAIM_PAID_&pays. as
             SELECT DISTINCT country,
                    COVER,
                    scheme ,
                    Type_Insurance,
                    Entity_CD2 as Entity_CD ,
                    Entity,
                    Cohort,
                    Quarter,
                    sum(Totl_Amnt_Pd) as Claim_Paid
                                                       
     FROM    CLAIM_PAID_&pays.
     group by Country,COVER,scheme,Type_Insurance,Entity_CD,Entity,Cohort, Quarter
      ; 
 quit;
 
 
 
proc sql ; 
create table CLAIM_PAID_&pays._1 as
select 
t1.*,
t2.Currency AS Currency_code
from CLAIM_PAID_&pays.  t1 
left join FX t2 on (t1.country=t2.country )     
;
quit ; 

data CLAIM_PAID_&pays._1;
set CLAIM_PAID_&pays._1 ;
if Entity_CD in (102,122,132142,152,172,192,212,302,312,502,682,702,712,772,782,792,802,812,821,822,832,842,851,852,861,862,872,882,892,902,912,922,931,932,942,951,952,961,962,972,982,992) then Entity = "FACL"; 
else if Entity_CD in (101,121,131,141,151,171,301,311,501,671,681,701,711,771,791,801,811,821,831,841,851,861,871,881,891,901,911,921,931,941,951,961,971,981,991) then Entity ="FICL"; 
else Entity ="UNKNOWN";
run;

data  CLAIM_PAID_&pays._2;
set  CLAIM_PAID_&pays._1;
Claim_Paid_Net=Claim_Paid;

run;



data CLAIM_FLUX_&pays. ;
set  CLAIM_PAID_&pays._2 ;
run;

PROC SQL;
	CREATE TABLE CLAIM_FLUX_&pays. as 
	SELECT DISTINCT
			h.*,
			s.partner_sales_name AS Agent
			FROM CLAIM_FLUX_&pays. h
	left JOIN TIA.CARTO_TIA s ON (h.Country=s.Country  and h.scheme=s.scheme)
	
	;
QUIT;


/********************************************************************************************************************************************/
/******************************************************     ACR BGD FORMAT -->   **************************************************************/ 
/********************************************************************************************************************************************/


data STOCK_CR_CLOT_TIA_&pays. ;

retain  Country Scheme  cover Entity_CD Entity Type_Insurance Cohort SURV  POSTE Currency_code NET_AMT Agent;
keep  Country Scheme Cohort cover SURV Entity_CD Entity Type_Insurance POSTE Currency_code NET_AMT Agent;
set STOCK_ACR_TIA_&pays. ;
where Entity not in ("TPA") ;
rename Agent=RGPT
       Cohort=GEN
 ;
SURV=substr(Quarter,1,4)*1;
run;


PROC SQL;
      create table TIA.STOCK_CR_CLOT_TIA_&pays. as
             SELECT DISTINCT country,
                    scheme ,
                    cover ,
                    Entity_CD ,
                    Entity,
                    Type_Insurance,
                    GEN,
                    SURV,
                    Currency_code,
                    POSTE,
                    sum(NET_AMT) as NET_AMT,
                    RGPT
                                                       
     FROM    STOCK_CR_CLOT_TIA_&pays.
     group by Country,scheme,COVER,Entity_CD,Entity,GEN, SURV, Currency_code,POSTE,RGPT
      ; 
 quit;


/********************************************************************************************************************************************/
/******************************************************     FORECAST BGD FORMAT -->   **************************************************************/ 
/********************************************************************************************************************************************/


data CLAIM_ACR_TIA_&pays._F  ;
keep Country cover scheme Entity_CD Entity Cohort Quarter Currency_code Type_Insurance Poste Claim_Paid_Net Agent ;
length Poste $40.;
set CLAIM_FLUX_&pays. ;
WHERE Entity IN ("FICL","FACL") ;
Rename Agent= RGPT
       Claim_Paid_Net=NET_AMT
       Cohort=GEN
;
if Cohort=. THEN Cohort=9999 ;
Poste="CLAIM" ;
run;

data STOCK_ACR_TIA_&pays._F  ;
keep Country cover scheme Entity_CD Entity Cohort Quarter Currency_code Type_Insurance Poste Agent NET_AMT ;
length Poste $40.;
set STOCK_ACR_TIA_&pays.  ;
Rename Agent= RGPT
       Cohort=GEN
;
if Cohort=. THEN Cohort=9999 ;
Poste="RESERVES" ;
run;

data HISTO_FLUX_PROVISIONS_&pays. ;
set CLAIM_ACR_TIA_&pays._F STOCK_ACR_TIA_&pays._F ;
WHERE Entity not in ("TPA") ;
run ;

PROC SQL;
      create table TIA.HISTO_FLUX_PROVISIONS_&pays. as
             SELECT DISTINCT country,
                    scheme ,
                    COVER,
                    Entity_CD ,
                    Entity,
                    Type_Insurance,
                    GEN,
                    Quarter,
                    Currency_code,
                    Poste,
                    sum(NET_AMT) as NET_AMT,
                    RGPT
                                                       
     FROM    HISTO_FLUX_PROVISIONS_&pays.
     group by Country,scheme,COVER,Entity_CD,Entity,GEN, Quarter,Currency_code,Poste,RGPT
      ; 
 quit;




 proc datasets lib=work memtype=DATA;   delete HISTO_FLUX_PROVISIONS_&pays. ;    run;
 proc datasets lib=work memtype=DATA;   delete CLAIM_ACR_TIA_&pays._F ;    run;
 proc datasets lib=work memtype=DATA;   delete STOCK_ACR_TIA_&pays._F ;    run;
 proc datasets lib=work memtype=DATA;   delete STOCK_ACR_TIA_&pays. ;    run;
 proc datasets lib=work memtype=DATA;   delete CLAIM_FLUX_&pays. ;    run;
 proc datasets lib=work memtype=DATA;   delete STOCK_ACR_TIA_&pays. ;    run;
 proc datasets lib=work memtype=DATA;   delete STOCK_CR_CLOT_TIA_&pays. ;    run;
 proc datasets lib=work memtype=DATA;   delete RESERVES_TIA_&pays._3 ;    run;
 proc datasets lib=work memtype=DATA;   delete RESERVES_TIA_&pays._2 ;    run;
 proc datasets lib=work memtype=DATA;   delete RESERVES_TIA_&pays._1 ;    run;
 proc datasets lib=work memtype=DATA;   delete RESERVES_TIA_&pays. ;    run; 
 proc datasets lib=work memtype=DATA;   delete CLAIM_PAID_&pays._2  ;    run;
 proc datasets lib=work memtype=DATA;   delete CLAIM_PAID_&pays._1  ;    run;
 proc datasets lib=work memtype=DATA;   delete CLAIM_PAID_&pays. ;    run;
 proc datasets lib=work memtype=DATA;   delete CR_&pays.  ;    run;
 proc datasets lib=work memtype=DATA;   delete IBNR_&pays.  ;    run;



%MEND;

%ACR(pays=DE) ;
%ACR(pays=DK) ;
%ACR(pays=IE) ;
%ACR(pays=IT) ;
%ACR(pays=FI) ;
%ACR(pays=FR) ;
%ACR(pays=GR) ;
%ACR(pays=NI) ;
%ACR(pays=NL) ;
%ACR(pays=NO) ;
%ACR(pays=PL) ;
%ACR(pays=PT) ;
%ACR(pays=SE) ;
%ACR(pays=TR) ;
%ACR(pays=UK) ;
%ACR(pays=CH) ;
%ACR(pays=ES) ;
%ACR(pays=AT) ;
%ACR(pays=CO) ;
%ACR(pays=MX) ;
%ACR(pays=BE) ;
%ACR(pays=LT) ;
