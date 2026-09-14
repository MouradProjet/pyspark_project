/*####################################################*/
/*################### INVENTAIRE TIA #################*/
/*####################################################*/

%MACRO IMPORT_EXCEL(DATAFILE=,OUT=,ONGLET=);PROC IMPORT DBMS=XLS DATAFILE= &DATAFILE. OUT= &OUT. REPLACE;  ;SHEET=&ONGLET. ;RUN;%MEND;
%MACRO IMPORT_EXCELX(DATAFILE=,OUT=,ONGLET=);PROC IMPORT  	DBMS=XLSX DATAFILE= &DATAFILE. OUT= &OUT. REPLACE;  ; SHEET=&ONGLET. ; RUN;%MEND;
%macro EXPORT_EXCEL(DATABASE=, DATATABLE=, SHEET=, );PROC EXPORT DATA = &DATATABLE. OUTFILE=&DATABASE. DBMS=XLS REPLACE ;SHEET = &SHEET.;run;%MEND;
%macro EXPORT_EXCELX(DATABASE=, DATATABLE=, SHEET=, );PROC EXPORT DATA = &DATATABLE. OUTFILE=&DATABASE. DBMS=XLSX REPLACE ;SHEET = &SHEET. ;run;%MEND;

/*#########################################################################*/
/*################### 2ème Etape: Extraction des données  #################*/
/*#########################################################################*/

%let LReseau =  ~/NAS/X ; /* Mettre le serveur approprié  entre -> ~/NAS/X  ou -> X:/Inventprev ** */
%LET Arrete = 2026_09_Q4;

LIBNAME TIA "&LReseau./08.Progammes/INTERNATIONAL/06_Inventaire CLP/&Arrete./02_Elements_Techniques/TIA/Extraction Donnees/TIA";

/* on uniformise les variables*/

%macro extract_data(Country=,) ; 

/* check #1 : 

/*################ GL Ledger ##############*/

/* %let Country = DE ; */

/**************************************************************************************/

proc sql; create table GLOBAL_PL_&Country._AGREG_2 as 		/* Le montant rei est bien égal au montant net que l'on veut calculer sauf pour les primes*/
select /*distinct*/
      gl_period,
      occurence_period, 
      scheme_id,
      scheme_version,
      cover_code_vorig as cover, 
      cohort_date as GEN ,
      (incident_date) as SURV format = ddmmyy10.,
      insurance_type_macro as ins_type, 
      local_currency as currency_code, 
      countryid_vorig as country, 
      axa_entity_code as entity_cd, 
      axa_risk_carrier as entity_name , 
      partner_name_vorig as AGENT_NAME_ORIG ,  
      kpi_name_0 as account_hierarchy_name,
      sum(amt_local_currency) FORMAT = NLBEST12. AS amt
          
from TIA.GLOBAL_PL_&Country.
group by   gl_period,occurence_period, scheme_id,scheme_version,cover_code_vorig, incident_date,cohort_date,insurance_type_macro, local_currency, countryid_vorig,axa_entity_code, axa_risk_carrier, partner_name_vorig, kpi_name_0 ;
quit; 


data GLOBAL_PL_&Country._AGREG_3 ;
FORMAT GEN2  DDMMYY10. ;
FORMAT SURV2  DDMMYY10. ;
length POSTE $40.;
length SCHEME $40.;
length entity $40.;
set GLOBAL_PL_&Country._AGREG_2 ;
if account_hierarchy_name in ("Claims Handling Fees - Direct","Claims Paid - Accepted","Claims Paid - Direct","Claims Handling Fees - Accepted","Claims Doctor Fees - Accepted","Claims Doctor Fees - Direct") 
then POSTE="CLAIM" ; 
ELSE if account_hierarchy_name in ("Gross Written Premium Cancellations - Direct","Gross Written Premium Cancellations - Accepted","Gross Written Premiums gross of Cancellations - Accepted","Gross Written Premiums gross of Cancellations - Direct","Gross Written Premiums gross of Cancellations - Ceded") 
then POSTE="PREMIUM";
ELSE if account_hierarchy_name in ("Gross Commission Cancellations - Direct","Gross Commission Cancellations - Accepted","Gross Commission Cancellations - Ceded","Gross Commissions - Accepted","Gross Commissions - Direct","Gross Commissions - Ceded") 
then POSTE ="COMMISSION";
ELSE if account_hierarchy_name in ("Profit Share - Direct","Profit Share - Accepted" ,"Profit Share - BLE Settlement") 
then POSTE ="PS_PAID";
ELSE if account_hierarchy_name in ("Claims Paid - Ceded") 
then POSTE ="CLAIM_CED";
Else POSTE="";

if ins_type in ("DIRECT UNDERWRITER","DIRECT INSURER") then Type_Insurance=0; *changement du HY'26 j'ai ajouter le direct insurer; 
Else if ins_type = "REINSURER"  then Type_Insurance = 4;
Else if ins_type = "TPA ONLY"  then Type_Insurance = 4;

if account_hierarchy_name in  ("Claims Paid - Ceded","Gross Commissions - Ceded","Gross Written Premiums gross of Cancellations - Ceded") and Type_Insurance=0 then Type_Insurance=11;
if account_hierarchy_name in  ("Claims Paid - Ceded","Gross Commissions - Ceded","Gross Written Premiums gross of Cancellations - Ceded") and Type_Insurance=4 then Type_Insurance=8;

year_gen=substr(GEN,1,4)*1;
month_gen=substr(GEN,6,7)*1;
GEN2 = mdy(month_gen, 01, year_gen) ;
year_surv=substr(occurence_period,1,4)*1;
month_surv=substr(occurence_period,6,7)*1;
year_gl=substr(gl_period,1,4)*1;
month_gl=substr(gl_period,6,7)*1;
SURV2 = mdy(month_surv, 01, year_surv) ;

/*occurence_period2=cats(year_surv,month_surv) ;*/
if month_surv in (1,2,3,4,5,6,7,8,9)    THEN occurence_period2=cats(year_surv,"0",month_surv);
if month_surv in (10,11,12)             THEN occurence_period2=cats(year_surv,month_surv);


/*gl_period2=cats(year_gl,month_gl) ;*/
if month_gl in (1,2,3,4,5,6,7,8,9)    THEN gl_period2=cats(year_gl,"0",month_gl);
if month_gl in (10,11,12)             THEN gl_period2=cats(year_gl,month_gl);

entity=entity_name ;
SCHEME = compress(trim(scheme_id)||'.'||(trim(scheme_version )) );
run;

proc sql; create table DATABASE_&Country._PL as 		
select distinct
      country,
      cover,      
      entity_cd,
      gl_period2 as gl_period,
      occurence_period2 as occurrence_period,
      Type_Insurance,
      SCHEME,
      currency_code,
      entity_name,
      GEN2 as GEN, 
      SURV, 
      AGENT_NAME_ORIG as RGPT, 
      POSTE,
      sum(amt) AS MONTANT, 
      account_hierarchy_name, 
      0000 as claim_case_no, 
      entity_name as entity
         
from GLOBAL_PL_&Country._AGREG_3
WHERE POSTE ne "" 
group by    country, cover, entity_cd, gl_period2,occurence_period2, Type_Insurance, SCHEME, currency_code, entity_name, GEN2, SURV, AGENT_NAME_ORIG, POSTE, account_hierarchy_name, claim_case_no, entity;
quit; 

data TIA.DATABASE_&Country._PL ;
set  DATABASE_&Country._PL  ;
run;

proc datasets lib=work memtype=DATA;   delete DATABASE_&Country._PL ;            run;
proc datasets lib=work memtype=DATA;   delete GLOBAL_PL_&Country._AGREG_2 ;      run;
proc datasets lib=work memtype=DATA;   delete GLOBAL_PL_&Country._2;             run;
proc datasets lib=work memtype=DATA;   delete GLOBAL_PL_&Country._AGREG_3 ;      run;
%mend;


%extract_data(Country=AT);
%extract_data(Country=BE); 
%extract_data(Country=CH);
%extract_data(Country=CO); 
%extract_data(Country=DE);
%extract_data(Country=DK);
%extract_data(Country=ES);
%extract_data(Country=FI);
%extract_data(Country=FR);
%extract_data(Country=GR);
%extract_data(Country=IE);
%extract_data(Country=IT);
%extract_data(Country=LT);
%extract_data(Country=LU); 
%extract_data(Country=MX); 
%extract_data(Country=NI);
%extract_data(Country=NL);
%extract_data(Country=NO);
%extract_data(Country=PE); 
%extract_data(Country=PL);
%extract_data(Country=PT);
%extract_data(Country=SE);
%extract_data(Country=TR);
%extract_data(Country=UK);



/* Concaténation des bases par pays */
Data TIA.DATABASE_ALL_PL; 
set TIA.DATABASE_: ; 
run ;


/* Exlusion des schèmes de Ex-Macao */
%let Import_01="&LReseau./08.Progammes/INTERNATIONAL/06_Inventaire CLP/&Arrete./02_Elements_Techniques/TIA/Scheme Database/Input/SDB.xlsx" ;     
%MACRO IMPORT_EXCEL(FILE=,OUT=,ONGLET=,);
PROC IMPORT                 
DATAFILE= &FILE.
OUT= &OUT. 
DBMS=xlsx REPLACE;
SHEET=&ONGLET. ;
RUN;
%MEND;
%IMPORT_EXCEL(FILE=&Import_01.,OUT=flag_legacy,ONGLET="flag_legacy");



*On remplace par le Hash; 

/*proc sql ; 
create table DATABASE_ALL_PL_F as
select distinct
t1.*,
t8.Data_Validated AS flag_legacy, 
t8.RPP AS RPP,
t8.Agent_Id AS Agent_Id,
t8.agent_name
from TIA.DATABASE_ALL_PL  t1 
left join FLAG_LEGACY t8 on (t1.country=t8.country AND t1.SCHEME=t8.scheme and t1.cover=t8.Cover) ;
quit ; 

/* Pour bien isoler Macao il faut faire une combinaison des trois règles
1. Agent ID  between 75000 and 79999
2. Agent Name with _FOS in the name
3. RPP present  */

/*


DATA DATABASE_ALL_PL_F ;
set   DATABASE_ALL_PL_F ;
if RPP not in ("0","") or Agent_Id =: "75" or prxmatch('/FOS|_FOS/',Agent_Name) then LEGACY_SCHEME_BOOK="MACAO";
else LEGACY_SCHEME_BOOK="TIA";
run ;

DATA TIA.DATABASE_ALL_PL_F ;
set DATABASE_ALL_PL_F ;
RUN;


*/


data TIA.DATABASE_ALL_PL_F(rename=(Data_Validated=flag_legacy));
	format Data_Validated $1. RPP $14. Agent_ID $7. Agent_name $33. LEGACY_SCHEME_BOOK $5.;
    if _n_ = 1 then do;
        /* Création de la table HASH */
        declare hash h(dataset:"FLAG_LEGACY");
        h.defineKey("country", "scheme", "Cover");
        h.defineData("Data_Validated", "RPP", "Agent_ID" ,"Agent_name");
        h.defineDone();
    end;
 
    set TIA.DATABASE_ALL_PL;
	 
    /* si la jointure n'est pas trouvée */
    if h.find() ne 0 then  do;
	    Agent_name = "";
	    LEGACY_SCHEME_BOOK="TIA";
    end;
    else do;
	    if RPP not in ("0","") or Agent_Id =: "75" or prxmatch('/FOS|_FOS/',Agent_Name) then LEGACY_SCHEME_BOOK="MACAO";
		else LEGACY_SCHEME_BOOK="TIA";
    end;
run;

Data TIA.DATABASE_ALL_PL_F; 
set TIA.DATABASE_ALL_PL_F; 
if Scheme = "RCI.1" and Country = "IT" then LEGACY_SCHEME_BOOK = "TIA";
run; 





 