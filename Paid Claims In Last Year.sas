%let LReseau = X ;           /* Lettre du serveur "Inventprev" attention au majuscule et minuscule*/
%LET Arrete = 2023_09_Prov ; 

LIBNAME data "~/NAS/X/08.Progammes/INTERNATIONAL/06_Inventaire CLP/&Arrete./02_Elements_Techniques/TIA/Extraction Donnees/Claims Extracts";

%Let YearStart = mdy (09,30,2022);
%Let YearEnd = mdy (09,30,2023);

%Macro PaidsInLastYear(c);

PROC SQL;
Create Table Paids_&c. as select
p.Country, p.Legal_Entity format=$10. length=10, sum(-p.Currency_Amt) as Paid_claims
from DATA.&c._CLMTRNS p
left join DATA.&c._CLMHDR q on p.CLA_CASE_NO=q.CLA_CASE_NO
WHERE   &YearStart. <= p.Trans_Date <= &YearEnd. 
GROUP BY p.Country, p.Legal_Entity;
Quit;

proc append base = Paids  data= Paids_&c. FORCE; run;


%Mend;

%PaidsInLastYear(CH);
%PaidsInLastYear(DE);
%PaidsInLastYear(DK);
%PaidsInLastYear(ES);
%PaidsInLastYear(FI);
%PaidsInLastYear(FR);
%PaidsInLastYear(GR);
%PaidsInLastYear(IE);
%PaidsInLastYear(IT);
%PaidsInLastYear(NI);
%PaidsInLastYear(NL);
%PaidsInLastYear(NO);
%PaidsInLastYear(PL);
%PaidsInLastYear(PT);
%PaidsInLastYear(SE);
%PaidsInLastYear(TR);
%PaidsInLastYear(AT);
%PaidsInLastYear(UK);
%PaidsInLastYear(CO);
%PaidsInLastYear(MX);
%PaidsInLastYear(BE);

data PAIDS ;
set  WORK.PAIDS_CH WORK.PAIDS_AT 
     WORK.PAIDS_DE WORK.PAIDS_DK WORK.PAIDS_ES WORK.PAIDS_FI WORK.PAIDS_FR WORK.PAIDS_GR WORK.PAIDS_IE WORK.PAIDS_IT WORK.PAIDS_NI WORK.PAIDS_NL WORK.PAIDS_NO WORK.PAIDS_PL WORK.PAIDS_PT WORK.PAIDS_SE WORK.PAIDS_TR WORK.PAIDS_UK
     WORK.PAIDS_MX WORK.PAIDS_CO WORK.PAIDS_BE
;
run ;

data  DATA.PAIDS ;
set PAIDS ;
if Legal_Entity="FICL" then Legal_Entity="IARD";
ELSE if Legal_Entity="FACL" then Legal_Entity="VIE";
ELSE Legal_Entity=Legal_Entity ; 
run; 









