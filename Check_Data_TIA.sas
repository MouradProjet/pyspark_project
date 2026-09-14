
/*########################### DAAP   ###############################*/

%let LReseau =  ~/NAS/X ;
%LET Arrete = 2026_09_Q4;
LIBNAME TIA "&LReseau./08.Progammes/INTERNATIONAL/06_Inventaire CLP/&Arrete./02_Elements_Techniques/TIA/Extraction Donnees/TIA";

data DATABASE_ALL_PL_V3 ;
set TIA.DATABASE_ALL_PL ;
if Type_Insurance in (11,8) and POSTE in ("PREMIUM")  then delete ; 
drop Type_Insurance ;
run ; 

proc sql ; 
create table DATABASE_ALL_PL_V3 as 
select country,entity_name,POSTE,gl_period,sum(MONTANT) as MONTANT     
from DATABASE_ALL_PL_V3
group by country,entity_name,POSTE,gl_period;     
quit ; 

data DATABASE_ALL_PL_V3 ;
set DATABASE_ALL_PL_V3 ;
if POSTE="PREMIUM" then POSTE="GWP" ;
if POSTE="COMMISSION" then POSTE="Comms" ;
if POSTE="CLAIM" then POSTE="Claims" ;
/*if Type_Insurance in (11,8) then delete ; 
drop Type_Insurance ;*/
run ; 

proc sort data = DATABASE_ALL_PL_V3 nodupkey; by country entity_name POSTE gl_period; run;

proc transpose data=DATABASE_ALL_PL_V3 out=DATABASE_ALL_PL_V3 (drop=_NAME_ _LABEL_);
var MONTANT; id POSTE; by country entity_name POSTE gl_period ;
run; 

data DATABASE_ALL_PL_V3 ;
set DATABASE_ALL_PL_V3 ;
if Comms=. then Comms=0 ;
if GWP=. then GWP=0 ;
if Claims=. then Claims=0 ;
if PS_PAID=. then PS_PAID=0 ;
if CLAIM_CED=. then CLAIM_CED=0 ;
drop POSTE ;
run ; 


/******************* ROW COUNT ******************/
proc sql; create table count as 
    select countryid_vorig, count(*) as RowCount
    from tia.daap_level_1_dueonly
    group by countryid_vorig;
quit;