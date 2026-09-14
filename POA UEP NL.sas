
%let LReseau = X;         /* Lettre du serveur "Inventprev" attention au majuscule et minuscule*/
%LET Arrete = 2026_04_V2 ;
%LET Arrete3 = Q126 ;
%let fichier_import= Q1 26 POA File  - DAAP Team;
%let N = 2026; 


LIBNAME TIA "~/NAS/X/08.Progammes/INTERNATIONAL/06_Inventaire CLP/&Arrete./02_Elements_Techniques/TIA/Extraction Donnees/TIA";


/*########################### POA NL ###############################*/
/*############################################################################*/


Proc SQL ; 
	Create table  map_rgpt as
Select distinct country, SCHEME, RGPT
from tia.uep_clot_tia_nl;
quit;



*attention AR : j'ai changé la surv en N-1 car sinon car me crée des GEP neg sur le CY / ca sera à revoir; 

%MACRO IMPORT_EXCELX(FILE=,OUT=,ONGLET=,);
PROC IMPORT DATAFILE= &FILE. 
OUT= &OUT. DBMS=xlsx REPLACE; 
SHEET=&ONGLET. ; 
RANGE = "J3:O30";
RUN;


Proc SQL ;
Create table &OUT. as
Select distinct t1.*,"NL" as country ,&N.-1 as  GEN , &N.-1 as SURV ,t2.RGPT
From  &OUT. t1
left join map_rgpt t2 on (t1.SCHEME=t2.SCHEME) ;
Quit;

%MEND;

%let Import_05="~/NAS/X/08.Progammes/INTERNATIONAL/06_Inventaire CLP/&Arrete./02_Elements_Techniques/TIA/Arrete reel/GEP/POA/&fichier_import..xlsx";
%IMPORT_EXCELX(FILE=&Import_05.,OUT=BGD_UEP_DAC_&Arrete3._NL_poa,ONGLET=&Arrete3.);



%macro UEP_DAC(POSTE);
Proc SQL ; 
	Create table &POSTE._RU as
	Select country ,SCHEME , "RU" as cover, 101 as entity_cd, "FICL" as entity ,0 as Type_Insurance, "EUR" as currency_code ,GEN ,SURV ,RGPT ,"&POSTE." as POSTE, &POSTE._RU as NET_AMT
	From  BGD_UEP_DAC_&Arrete3._NL_poa ;
Quit;

Proc SQL ; 
	Create table &POSTE._DA as
	Select country ,SCHEME , "DA" as cover, 101 as entity_cd, "FICL" as entity ,0 as Type_Insurance, "EUR" as currency_code ,GEN ,SURV ,RGPT ,"&POSTE." as POSTE, &POSTE._DA as NET_AMT
	From  BGD_UEP_DAC_&Arrete3._NL_poa ;
Quit;

DATA &POSTE._CLOT_TIA_NL_poa;
	set &POSTE._RU  &POSTE._DA;
run;
%mend;
%UEP_DAC(UEP);
%UEP_DAC(DAC);

DATA TIA.UEP_CLOT_TIA_NL_2 ;
SET  tia.uep_clot_tia_nl uep_clot_tia_nl_poa ;
RUN ;
DATA TIA.DAC_CLOT_TIA_NL_2 ;
SET  tia.dac_clot_tia_nl  dac_clot_tia_nl_poa;
RUN ;






