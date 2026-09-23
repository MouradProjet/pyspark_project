/* Macro import & export */ 
%MACRO IMPORT_EXCEL(DATAFILE=,OUT=,ONGLET=,);
PROC IMPORT                 
DATAFILE= &DATAFILE.
OUT= &OUT. 
DBMS=xlsx REPLACE;
SHEET=&ONGLET. ;
RUN;
%MEND;

%macro EXPORT_EXCEL(DATABASE=, DATATABLE=, SHEET=, );
PROC EXPORT  	
DATA = &DATATABLE.
OUTFILE=&DATABASE. 
DBMS=XLS REPLACE ;
SHEET = &SHEET. ;
run;
%MEND;
/*************************/


/* à mettre à jour */ 
%let LReseau = ~/NAS/X ;
%LET DT_CAL= "31dec2026"d; 
%LET DT_Arrete_Reel = "27MAR2026"d ;
%LET Arrete = 2026_04_V2 ; 
%LET N = 2026 ; 
/*******************/ 


LIBNAME Out_GEP "&LReseau./08.Progammes/INTERNATIONAL/06_Inventaire CLP/&Arrete./02_Elements_Techniques/TIA/Arrete reel/GEP/Output/DAAP" ;
LIBNAME TIA "&LReseau./08.Progammes/INTERNATIONAL/06_Inventaire CLP/&Arrete./02_Elements_Techniques/TIA/Extraction Donnees/TIA";
LIBNAME LR "&LReseau./08.Progammes/INTERNATIONAL/06_Inventaire CLP/&Arrete./02_Elements_Techniques/TIA/Arrete reel/RESERVES/ON-SYSTEM/CASES RESERVES/Output";

*Import de la réas; 
%let repimp=&LReseau./08.Progammes/INTERNATIONAL/06_Inventaire CLP/&Arrete./02_Elements_techniques/TIA/Arrete reel/RESERVES/ON-SYSTEM/CASES RESERVES/Model Properties;
%let nom=Reassurance.xlsx;
%IMPORT_EXCEL(DATAFILE="&repimp./&nom.",OUT=reas,ONGLET='Parametres_Reas')

*Import mapping cover; 
%let repimp=&LReseau./08.Progammes/INTERNATIONAL/06_Inventaire CLP/01_Mappings;
%let nom=Mapping cover TIA.xlsx;
%IMPORT_EXCEL(DATAFILE="&repimp./&nom.",OUT=mapping_cover,ONGLET='Feuil1')

*mapping des curreny; 
%let repimp=&LReseau./08.Progammes/INTERNATIONAL/06_Inventaire CLP/01_Mappings;
%let nom=Mapping_currency.xlsx;
%IMPORT_EXCEL(DATAFILE="&repimp./&nom.",OUT=mapping_currency,ONGLET='Mapping')



*mapping des cover; 
proc sql; create table mapping_cover_distinct as select distinct TIA_Cover, cover from mapping_cover quit;

*mappping des rgpt; 
proc sql; create table mapping_rgpt as select distinct Country, Scheme, partner_sales_name as RGPT from tia.carto_tia; quit; 

*mappping des typaff; 
proc sql; create table mapping_typaff as select distinct Country, Scheme, AXA_QS_Involvement as AXA_QS_Involvement,
(case when AXA_QS_Involvement = "DIRECT UNDERWRITER" then "DI"
      when AXA_QS_Involvement = "UNKNOWN" then "DI"
      when AXA_QS_Involvement = "COINSURER" then "CO"
      when AXA_QS_Involvement = "TPA ONLY" then "DI"
      when AXA_QS_Involvement = "REINSURER" then "RE" end ) as typidean,

(case when AXA_QS_Involvement = "DIRECT UNDERWRITER" then 0
      when AXA_QS_Involvement = "UNKNOWN" then 0
      when AXA_QS_Involvement = "COINSURER" then 2
      when AXA_QS_Involvement = "TPA ONLY" then 0
      when AXA_QS_Involvement = "REINSURER" then 4 end ) as typaff

from tia.carto_tia; quit; 

 


*Début des calculs par pays; 


%macro Calcul(country= ,cc=) ; 

/* %LET country = AUSTRIA ; %LET cc= AT;  */ 

Data policy_rule_&cc._&N.; 
set out_gep.policy_rule_&country._bgd ; 
where year(EXP_PERIOD) = &N. ; 
run; 

proc sql; create table policy_rule_&cc._&N._net as select t1.*, t2.QP_rei_PREMIUM as QP 
from policy_rule_&cc._&N. t1
left join reas t2 on t1.Country=t2.country AND compress(t1.PRODUCT||"."||t1.PRODUCT_VERSION)=t2.scheme AND t1.cover=t2.cover AND t1.GL_TYPE_NO=t2.Original_underwritter; 
quit; 

data policy_rule_&cc._&N._net; 
set policy_rule_&cc._&N._net; 
if QP ne . then UEP_TBT_net= UEP_TBT*QP; 
else UEP_TBT_net = UEP_TBT; 
run ; 


proc sql; create table fc_policy_rule_&cc. as select country, cover, product, product_version, policy_line_no, gl_type_no ,
sum ((case when month(EXP_PERIOD)= month(&DT_Arrete_Reel.) and year(EXP_PERIOD)=year(&DT_Arrete_Reel.) then UEP_TBT_net else 0 end)) as UEP_DT_arrete_reel,
sum ((case when month(EXP_PERIOD)= month(&DT_CAL.) and year(EXP_PERIOD)=year(&DT_CAL.) then UEP_TBT_net else 0 end)) as UEP_DT_cal
from policy_rule_&cc._&N._net
group by country, cover, product, product_version, policy_line_no, gl_type_no ;
quit; 

data fc_policy_rule_&cc._2; 
set fc_policy_rule_&cc.; 
UEP_for_forecast = UEP_DT_arrete_reel - UEP_DT_cal; *reste des UEP jusque décembre pour estimer les PSAP;  
run ;

proc sql; create table fc_policy_rule_&cc._3 as select t1.*, t2.Cover as Cover2, t3.RGPT , t4.LR , t5.currency , t6.typaff
from fc_policy_rule_&cc._2 t1
left join mapping_cover_distinct t2 on t1.cover = t2.TIA_Cover 
left join mapping_rgpt t3 on t1.country=t3.country and compress(t1.PRODUCT||"."||t1.PRODUCT_VERSION)=t3.Scheme
left join lr.indic_reel_all_net t4 on t1.country =t4.Country and t2.Cover=t4.cover_name and t3.RGPT = t4.RGPT
left join work.mapping_currency t5 on t1.country =t5.country 
left join mapping_typaff t6 on t1.country=t6.country and compress(t1.PRODUCT||"."||t1.PRODUCT_VERSION)=t6.Scheme;
quit; 

data fc_policy_rule_&cc._4;
set fc_policy_rule_&cc._3;
PSAP_FC =  UEP_for_forecast * LR ; 
run; 

*Format BGD; 
Proc sql; create table  TIA.reserves_bgd_fc_&cc._2_net as select distinct
Country as Country ,
compress(PRODUCT||"."||PRODUCT_VERSION) as Scheme, 
Cover2 as cover_name, 
(case when gl_type_no in (101,121,131,141,151,171,301,311,501,671,681,701,711,771,791,801,811,821,831,841,851,861,871,881,891,901,911,921,931,941,951,961,971,981,991) then "FICL" 
      when gl_type_no in (102,122,132,142,152,172,192,212,302,312,502,682,702,712,772,782,792,802,812,821,822,832,842,851,852,861,862,872,882,892,902,912,922,931,932,942,951,952,961,962,972,982,992) then "FACL" end) as entity, 

typaff as Type_Insurance, 
currency as currency_code, 
&N. as GEN, 
sum(UEP_for_forecast) as UEP_FC,
mean(LR) as LR,
&N. as SURV,
RGPT as RGPT, 
sum(PSAP_FC) as NET_AMT,
"RESERVES_CLOT" as POSTE 
from fc_policy_rule_&cc._4 
group by country, Scheme, cover_name, entity, Type_Insurance, currency_code, SURV, RGPT, POSTE;
quit; 

proc datasets lib=work memtype=DATA;   delete Policy_Rule_&cc._&N.;   run;
proc datasets lib=work memtype=DATA;   delete fc_policy_rule_&cc.;   run;
proc datasets lib=work memtype=DATA;   delete fc_policy_rule_&cc._2;   run;
proc datasets lib=work memtype=DATA;   delete fc_policy_rule_&cc._3;   run;


%mend ; 
    
%Calcul(country = NETHERLANDS, cc = NL) ;           
%Calcul(country = POLAND, cc = PL ) ;    
%Calcul(country = NORTHERNIRELAND, cc = NI) ;         
%Calcul(country = NORWAY, cc = NO) ; 
%Calcul(country = FINLAND, cc = FI) ; 
%Calcul(country = ESTONIA, cc = EE) ; 
%Calcul(country = COLOMBIA, cc = CO) ; 
%Calcul(country = PERU, cc = PE) ; 
%Calcul(country = LATVIA, cc = LV) ; 
%Calcul(country = LITHUANIA, cc = LT) ; 
%Calcul(country = MEXICO, cc = MX) ; 
%Calcul(country = GREECE, cc = GR) ;
%Calcul(country = GERMANY, cc = DE) ; 
%Calcul(country = SPAIN, cc = ES) ;  
%Calcul(country = DENMARK, cc = DK) ; 
%Calcul(country = TURKEY, cc = TR) ; 
%Calcul(country = SWEDEN, cc = SE) ; 
%Calcul(country = UK, cc = UK) ;
%Calcul(country = SWITZERLAND, cc = CH) ; 
%Calcul(country = IRELAND, cc = IE) ; 
%Calcul(country = AUSTRIA, cc = AT) ; 
%Calcul(country = BELGIUM, cc = BE) ; 


%Calcul(country = PORTUGAL1, cc = PT1) ;
%Calcul(country = PORTUGAL2, cc = PT2) ;
%Calcul(country = PORTUGAL31, cc = PT31) ;
%Calcul(country = PORTUGAL32, cc = PT32) ;
%Calcul(country = PORTUGAL4, cc = PT4) ;
%Calcul(country = PORTUGAL51, cc = PT51) ;
%Calcul(country = PORTUGAL52, cc = PT52) ;

*merge du PT; 
data TIA.reserves_bgd_fc_PT_2_net; 
set TIA.reserves_bgd_fc_PT1_2_net
TIA.reserves_bgd_fc_PT2_2_net
TIA.reserves_bgd_fc_PT31_2_net
TIA.reserves_bgd_fc_PT32_2_net
TIA.reserves_bgd_fc_PT4_2_net
TIA.reserves_bgd_fc_PT51_2_net
TIA.reserves_bgd_fc_PT52_2_net; 
run; 


%Calcul(country = ITALY101, cc = IT101) ;
%Calcul(country = ITALY102, cc = IT102) ;

%Calcul(country = ITALY11, cc = IT11) ;
%Calcul(country = ITALY12, cc = IT12) ;

%Calcul(country = ITALY31, cc = IT31) ;
%Calcul(country = ITALY32, cc = IT32) ;

%Calcul(country = ITALY41, cc = IT41) ;
%Calcul(country = ITALY42, cc = IT42) ;

%Calcul(country = ITALY51, cc = IT51) ;
%Calcul(country = ITALY52, cc = IT52) ;

%Calcul(country = ITALY61, cc = IT61) ;
%Calcul(country = ITALY62, cc = IT62) ;

%Calcul(country = ITALY71, cc = IT71) ;
%Calcul(country = ITALY72, cc = IT72) ;

%Calcul(country = ITALY81, cc = IT81) ;
%Calcul(country = ITALY82, cc = IT82) ;

%Calcul(country = ITALY91, cc = IT91) ;
%Calcul(country = ITALY92, cc = IT92) ;

*merge de l'IT; 
data TIA.reserves_bgd_fc_IT_2_net; 
set TIA.reserves_bgd_fc_IT101_2_net
TIA.reserves_bgd_fc_IT102_2_net
TIA.reserves_bgd_fc_IT11_2_net
TIA.reserves_bgd_fc_IT12_2_net
TIA.reserves_bgd_fc_IT31_2_net
TIA.reserves_bgd_fc_IT32_2_net
TIA.reserves_bgd_fc_IT41_2_net
TIA.reserves_bgd_fc_IT42_2_net
TIA.reserves_bgd_fc_IT51_2_net
TIA.reserves_bgd_fc_IT52_2_net
TIA.reserves_bgd_fc_IT61_2_net
TIA.reserves_bgd_fc_IT62_2_net
TIA.reserves_bgd_fc_IT71_2_net
TIA.reserves_bgd_fc_IT72_2_net
TIA.reserves_bgd_fc_IT81_2_net
TIA.reserves_bgd_fc_IT82_2_net
TIA.reserves_bgd_fc_IT91_2_net
TIA.reserves_bgd_fc_IT92_2_net; 
run; 

/* FIN */ 

*merge des pays; 
data TIA.reserves_bgd_fc_all_net; 
set TIA.reserves_bgd_fc_NL_2_net
TIA.reserves_bgd_fc_PL_2_net
TIA.reserves_bgd_fc_NI_2_net
TIA.reserves_bgd_fc_NO_2_net
TIA.reserves_bgd_fc_FI_2_net
TIA.reserves_bgd_fc_EE_2_net
TIA.reserves_bgd_fc_CO_2_net
TIA.reserves_bgd_fc_PE_2_net
TIA.reserves_bgd_fc_LV_2_net
TIA.reserves_bgd_fc_LT_2_net
TIA.reserves_bgd_fc_MX_2_net
TIA.reserves_bgd_fc_GR_2_net
TIA.reserves_bgd_fc_DE_2_net
TIA.reserves_bgd_fc_ES_2_net
TIA.reserves_bgd_fc_DK_2_net
TIA.reserves_bgd_fc_TR_2_net
TIA.reserves_bgd_fc_SE_2_net
TIA.reserves_bgd_fc_UK_2_net
TIA.reserves_bgd_fc_CH_2_net
TIA.reserves_bgd_fc_IE_2_net
TIA.reserves_bgd_fc_AT_2_net
TIA.reserves_bgd_fc_BE_2_net
TIA.reserves_bgd_fc_PT_2_net
TIA.reserves_bgd_fc_IT_2_net
;run; 

 
	  







