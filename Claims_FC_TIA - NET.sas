
%let LReseau = X; /* Lettre du serveur "Inventprev" attention au majuscule et minuscule*/
%LET Arrete = 2026_04_V2 ;
%LET Arrete1 = 2026_04_V2 ;  
%let N = 2026 ;
%Let An_Ref = 2025 ; 
%let Ouput=CR_Q126;

LIBNAME TIA "~/NAS/&LReseau./08.Progammes/INTERNATIONAL/06_Inventaire CLP/&Arrete./02_Elements_Techniques/TIA/Extraction Donnees/TIA";
LIBNAME &Ouput. "~/NAS/&LReseau./08.Progammes/INTERNATIONAL/06_Inventaire CLP/&Arrete1./02_Elements_Techniques/TIA/Arrete reel/RESERVES/ON-SYSTEM/CASES RESERVES/Output";

Proc SQl;
create table Ref_Cover AS
select distinct cover as covmd_cover_code, cover_name
from TIA.carto_tia;
quit;

data Ref_Cover;
set Ref_Cover;
if covmd_cover_code="DT" and cover_name = "null" then delete;
run;


/****************************************************************************************************************/
%macro solde_tech(Country=) ; 
/* %Let Country = DK ; */

data UEP_&country._Forecast ;
set TIA.UEP_&country._Forecast;
where SI ="TIA";
run;

Proc SQl;
create table UEP_&country._Forecast AS
select distinct 
t1.country, t1.SCHEME, t3.cover_name, t1.entity, t1.Type_Insurance, t1.currency_code, t1.GEN, t1.SURV, t1.RGPT, t1.POSTE, sum(t1.MONTANT) AS MONTANT
from UEP_&country._Forecast t1 Left Join Ref_Cover t3 on (t1.cover = t3.covmd_cover_code)
group by t1.country, t1.SCHEME, t3.cover_name, t1.entity, t1.Type_Insurance, t1.currency_code, t1.GEN, t1.SURV, t1.RGPT, t1.POSTE;
quit;


/******************* CLAIMS ******************/
data GWP_BGD_FC_&Country._1 ;
set TIA.GWP_BGD_FC_&Country;
where SI ="TIA";
run;

data GWP_BGD_FC_&Country._1 ;
set  GWP_BGD_FC_&Country._1 ;
where Source   not IN  ("NEW_B") ;
drop RGPT ;
run ;

proc SQL;
Create table GWP_BGD_FC_&Country._1 AS
select distinct t1.*, b.partner_sales_name as RGPT 
from GWP_BGD_FC_&Country._1 t1 
Left Join TIA.CARTO_TIA b On (t1.country=b.Country and t1.SCHEME=b.SCHEME and t1.cover=b.cover );
quit;

proc SQL;
Create table GWP_BGD_FC_&Country._1 AS
select distinct t1.country, t1.SCHEME, t1.cover_name, t1.entity, t1.Type_Insurance, t1.currency_code, t1.GEN, t1.SURV,  t1.POSTE, sum(t1.NET_AMT) as NET_AMT,t1.Source, t1.RGPT
from GWP_BGD_FC_&Country._1 t1 
group by  t1.country, t1.SCHEME, t1.cover_name, t1.entity, t1.Type_Insurance, t1.currency_code, t1.GEN, t1.SURV, t1.RGPT;
quit;

data GWP_BGD_FC_&Country._1;
set GWP_BGD_FC_&Country._1;
RGPT_Code =scan(RGPT,1,"~");
DROP RGPT;
rename RGPT_Code=RGPT;
run;

Data GWP_BGD_FC_&Country._1 ;
set  GWP_BGD_FC_&Country._1 ;
if   RGPT in ("Bankintercard~","Bankintercard") then  RGPT="Bankintercards" ;
run ;

data GWP_BGD_FC_&Country._2 ;
set TIA.GWP_BGD_FC_&Country;
where SI ="TIA";
run;

proc SQL;
Create table GWP_BGD_FC_&Country._2 AS
select distinct t1.country, t1.SCHEME, t1.cover_name, t1.entity, t1.Type_Insurance, t1.currency_code, t1.GEN, t1.SURV,  t1.POSTE, sum(t1.NET_AMT) as NET_AMT,t1.Source, t1.RGPT
from GWP_BGD_FC_&Country._2 t1 
where t1.Source  in  ("NEW_B") 
group by  t1.country, t1.SCHEME, t1.cover_name, t1.entity, t1.Type_Insurance, t1.currency_code, t1.GEN, t1.SURV, t1.RGPT;
quit;

proc SQL;
Create table Charge_BGD_FC_&Country._1 AS
select distinct t1.country, t1.SCHEME, t1.cover_name, t1.entity, t1.Type_Insurance, t1.currency_code, t1.GEN,t1.NET_AMT as GWP_FC,t2.MONTANT as UEP_FC,b.LR, t1.SURV, t1.RGPT, "CLAIM" AS POSTE,
(CASE 
 when (b.LR is null or abs(b.LR) > 3 or b.LR < 0) then 0.50 * (t1.NET_AMT - COALESCE(t2.MONTANT, 0))
            else b.LR * (t1.NET_AMT - COALESCE(t2.MONTANT, 0))   end) as NET_AMT /*, t1.entity_cd */ 
from GWP_BGD_FC_&Country._1 t1 
Left Join &Ouput..INDIC_REEL_ALL_net b	on (t1.country=b.country and t1.cover_name=b.cover_name and t1.RGPT=b.RGPT) 
Left Join UEP_&country._Forecast t2	on (t1.country=t2.country and t1.cover_name=t2.cover_name and t1.SCHEME=t2.Scheme and t1.entity = t2.entity)
where t1.Source NOT IN   ("NEW_B")
group by t1.country, t1.SCHEME, t1.cover_name, t1.entity, t1.Type_Insurance, t1.currency_code, t1.GEN,b.LR, t1.SURV, t1.RGPT;
quit;

Proc SQL;
create table CARTO_NB AS 
select distinct t1.country, t1.SCHEME, t1.Type_Insurance,t1.RGPT, t1.cover, t1.Duration, t1.Comm_rate, t1.PS_rate, t1.LR_rate, t2.cover_name AS cover_name
from TIA.CARTO_NB t1
Left join Ref_Cover t2 ON (t1.COVER = t2.covmd_cover_code) ;
quit ;

proc SQL;
Create table Charge_BGD_FC_&Country._2 AS
select distinct t1.country, t1.SCHEME, t1.cover_name, t1.entity, t1.Type_Insurance, t1.currency_code, t1.NET_AMT as GWP_FC, t2.MONTANT as UEP_FC,b.LR_rate as LR,t1.GEN, t1.SURV, t1.RGPT, "CLAIM" AS POSTE, 
(CASE when (b.LR_rate is null or abs(b.LR_rate)>3 or b.LR_rate<0 ) then 0.50 * (t1.NET_AMT - COALESCE(t2.MONTANT, 0))
else b.LR_rate * (t1.NET_AMT - COALESCE(t2.MONTANT, 0))   end) as NET_AMT /*, t1.entity_cd */ 
from GWP_BGD_FC_&Country._2 t1 
/*ajout du scheme dans la clé du premier left join car cela me faisait des doublons*/ 
Left join CARTO_NB b ON (t1.SCHEME=B.SCHEME and t1.country = b.country and t1.cover_name = b.cover_name and t1.RGPT=b.RGPT and t1.Type_Insurance=b.Type_Insurance)
Left Join UEP_&country._Forecast t2	on (t1.country=t2.country and t1.cover_name=t2.cover_name and t1.SCHEME=t2.Scheme and t1.Type_Insurance=t2.Type_Insurance and t1.entity = t2.entity)
where t1.Source not in  ("STOCK") 
group by  t1.country, t1.SCHEME, t1.cover_name, t1.entity, t1.Type_Insurance, t1.currency_code, t1.GEN, b.LR_rate,t1.SURV, t1.RGPT;;
quit;

data Charge_BGD_FC_&Country. ;
set Charge_BGD_FC_&Country._1 Charge_BGD_FC_&Country._2 ;
run ;


/* CLAIM = CHARGE */

/****************************************************************************************************************/
Data Repart_Prov_&Country.;
set TIA.HISTO_FLUX_PROVISIONS_&Country.;
where substr (Quarter,1,4)="&An_Ref.";
run;

Proc SQL; 
Create table Repart_Charge_&Country. As
Select t1.Country, t1.scheme, t2.cover_name, t1.Poste, sum(t1.NET_AMT) as AMT
From Repart_Prov_&Country. t1 Left join Ref_Cover t2 on (t1.cover=t2.covmd_cover_code)
Where Quarter LIKE "%&An_Ref.%"
Group By t1.Country, t1.scheme, t2.cover_name, t1.Poste
Order By t1.Country, t1.scheme, t2.cover_name, t1.Poste;
Quit;

Proc Transpose Data=Repart_Charge_&Country. Out=Repart_Charge_&Country._2 (Drop=_NAME_ _LABEL_);
Var AMT;
Id Poste;
By Country scheme cover_name;
Run;

Data Repart_Charge_&Country._3; Set Repart_Charge_&Country._2;
If CLAIM = . Then CLAIM = 0;
If RESERVES = . Then RESERVES = 0;
Tx_Claims_Reserve = RESERVES / (CLAIM + RESERVES);
Tx_Claims_Paid = 1 - Tx_Claims_Reserve;
Run;

Proc SQL; Create Table Charge_BGD_FC_2_&Country. As
Select a.*, (Case When b.Tx_Claims_Reserve = . Then 0.5 else b.Tx_Claims_Reserve end) As Tx_Claims_Reserve,
		(Case When b.Tx_Claims_Paid = . Then 0.5 else b.Tx_Claims_Paid end) As Tx_Claims_Paid
From Charge_BGD_FC_&Country. a 
Left Join REPART_CHARGE_&Country._3 b On (a.country=b.Country and a.SCHEME=b.SCHEME and a.cover_name=b.cover_name);
Quit;

Data TIA.Charge_BGD_FC_2_&Country._net; 
Set Charge_BGD_FC_2_&Country.; 
run;

Data TIA.RESERVES_BGD_FC_&Country._net; 
Set Charge_BGD_FC_2_&Country.;
NET_AMT_2 = NET_AMT * Tx_Claims_Reserve;
attrib POSTE2 length=$ 20;
If POSTE In ('CLAIM') Then POSTE2 = ('RESERVES_CLOT'); /* Warning : splitt in PTEC/PSAP depend on cover */
Drop NET_AMT Tx_Claims_Reserve Tx_Claims_Paid POSTE GWP_FC;
Rename POSTE2 = POSTE;
Rename NET_AMT_2=NET_AMT;
/*if NET_AMT_2 < 0 then delete ;*/
Run;

Data TIA.CLAIMSPAID_BGD_FC_&Country._net; 
Set Charge_BGD_FC_2_&Country.;
NET_AMT_2 = NET_AMT * Tx_Claims_Paid;
Drop NET_AMT Tx_Claims_Reserve Tx_Claims_Paid GWP_FC;
Rename NET_AMT_2=NET_AMT;
/*if NET_AMT_2 < 0 then delete ;*/
Run;

%mend;

%solde_tech(Country=DE);
%solde_tech(Country=AT);
%solde_tech(Country=DK);
%solde_tech(Country=ES);
%solde_tech(Country=GR);
%solde_tech(Country=IT);
%solde_tech(Country=FI);
%solde_tech(Country=IE); 
%solde_tech(Country=IT);
%solde_tech(Country=NL);
%solde_tech(Country=NO);
%solde_tech(Country=PL);
%solde_tech(Country=PT);
%solde_tech(Country=SE);
%solde_tech(Country=TR);
%solde_tech(Country=UK);
%solde_tech(Country=CH);
%solde_tech(Country=CO);
%solde_tech(Country=MX);
%solde_tech(Country=BE);
%solde_tech(Country=LT);


/* spécial pour la FR */
%Let Country = FR ;

data GWP_BGD_FC_&Country._1 ;
set TIA.GWP_BGD_FC_&Country;
where SI ="TIA";
run;

data GWP_BGD_FC_&Country._1 ;
set  GWP_BGD_FC_&Country._1 ;
where Source   not IN  ("NEW_B") ;
drop RGPT ;
run ;

proc SQL;
Create table GWP_BGD_FC_&Country._1 AS
select distinct t1.*, b.partner_sales_name as RGPT 
from GWP_BGD_FC_&Country._1 t1 
Left Join TIA.CARTO_TIA b On (t1.country=b.Country and t1.SCHEME=b.SCHEME and t1.cover=b.cover);
quit;

proc SQL;
Create table GWP_BGD_FC_&Country._1 AS
select distinct t1.country, t1.SCHEME, t1.cover_name, t1.entity, t1.Type_Insurance, t1.currency_code, t1.GEN, t1.SURV,  t1.POSTE, sum(t1.NET_AMT) as NET_AMT,t1.Source, t1.RGPT
from GWP_BGD_FC_&Country._1 t1 
group by  t1.country, t1.SCHEME, t1.cover_name, t1.entity, t1.Type_Insurance, t1.currency_code, t1.GEN, t1.SURV, t1.RGPT;
quit;

data GWP_BGD_FC_&Country._2 ;
set TIA.GWP_BGD_FC_&Country;
where SI ="TIA";
run;

proc SQL;
Create table GWP_BGD_FC_&Country._2 AS
select distinct t1.country, t1.SCHEME, t1.cover_name, t1.entity, t1.Type_Insurance, t1.currency_code, t1.GEN, t1.SURV,  t1.POSTE, sum(t1.NET_AMT) as NET_AMT,t1.Source, t1.RGPT
from GWP_BGD_FC_&Country._2 t1 
where t1.Source  in  ("NEW_B") 
group by  t1.country, t1.SCHEME, t1.cover_name, t1.entity, t1.Type_Insurance, t1.currency_code, t1.GEN, t1.SURV, t1.RGPT;
quit;

proc SQL;
Create table Charge_BGD_FC_&Country._1 AS
select distinct t1.country, t1.SCHEME, t1.cover_name, t1.entity, t1.Type_Insurance, t1.currency_code,t1.NET_AMT as GWP_FC, t1.GEN,b.LR, t1.SURV, t1.RGPT, "CLAIM" AS POSTE,
(CASE when (b.LR=. or abs(b.LR)>3 or b.LR<0 ) then 0.50 * (t1.NET_AMT /*- (case when t2.MONTANT=. then 0 else t2.MONTANT END)*/)
else b.LR * (t1.NET_AMT/*-(case when t2.MONTANT=. then 0 else t2.MONTANT END)*/) end) AS NET_AMT /*, t1.entity_cd */ 
from GWP_BGD_FC_&Country._1 t1 
Left Join &Ouput..INDIC_REEL_ALL_net b	on (t1.country=b.country and t1.cover_name=b.cover_name and t1.RGPT=b.RGPT) 
where t1.Source NOT IN   ("NEW_B")
group by  t1.country, t1.SCHEME, t1.cover_name, t1.entity, t1.Type_Insurance, t1.currency_code, t1.GEN,b.LR, t1.SURV, t1.RGPT;
quit;

Proc SQL;
create table CARTO_NB AS 
select distinct t1.country, t1.SCHEME, t1.Type_Insurance,t1.RGPT, t1.cover, t1.Duration, t1.Comm_rate, t1.PS_rate, t1.LR_rate, t2.cover_name AS cover_name
from TIA.CARTO_NB t1
Left join Ref_Cover t2 ON (t1.COVER = t2.covmd_cover_code) ;
quit ;

proc SQL;
Create table Charge_BGD_FC_&Country._2 AS
select distinct t1.country, t1.SCHEME, t1.cover_name, t1.entity, t1.Type_Insurance, t1.currency_code,t1.NET_AMT as GWP_FC, t1.GEN, b.LR_rate as LR,t1.SURV, t1.RGPT, "CLAIM" AS POSTE, 
(CASE when (b.LR_rate=. or abs(b.LR_rate)>3 or b.LR_rate<0 ) then 0.50 * (t1.NET_AMT /*- (case when t2.MONTANT=. then 0 else t2.MONTANT END)*/)
else b.LR_rate * (t1.NET_AMT/*-(case when t2.MONTANT=. then 0 else t2.MONTANT END)*/) end) AS NET_AMT /*, t1.entity_cd */ 
from GWP_BGD_FC_&Country._2 t1 
Left join CARTO_NB b ON (t1.SCHEME=B.SCHEME and t1.country = b.country AND t1.cover_name = b.cover_name and t1.RGPT=b.RGPT and t1.Type_Insurance=b.Type_Insurance )
where t1.Source not in  ("STOCK")  
group by  t1.country, t1.SCHEME, t1.cover_name, t1.entity, t1.Type_Insurance, t1.currency_code, t1.GEN, b.LR_rate, t1.SURV, t1.RGPT;;
quit;


data Charge_BGD_FC_&Country. ;
set Charge_BGD_FC_&Country._1 Charge_BGD_FC_&Country._2 ;
run ;

/* CLAIM = CHARGE */

Data Repart_Prov_&Country.;
set TIA.HISTO_FLUX_PROVISIONS_&Country.;
where substr (Quarter,1,4)="&An_Ref.";
run;

Proc SQL; 
Create table Repart_Charge_&Country. As
Select t1.Country, t1.scheme, t2.cover_name, t1.Poste, sum(t1.NET_AMT) as AMT
From Repart_Prov_&Country. t1 Left join Ref_Cover t2 on (t1.cover=t2.covmd_cover_code)
Where Quarter LIKE "%&An_Ref.%"
Group By t1.Country, t1.scheme, t2.cover_name, t1.Poste
Order By t1.Country, t1.scheme, t2.cover_name, t1.Poste;
Quit;

Proc Transpose Data=Repart_Charge_&Country. Out=Repart_Charge_&Country._2 (Drop=_NAME_ _LABEL_);
Var AMT;
Id Poste;
By Country scheme cover_name;
Run;

Data Repart_Charge_&Country._3; Set Repart_Charge_&Country._2;
If CLAIM = . Then CLAIM = 0;
If RESERVES = . Then RESERVES = 0;
Tx_Claims_Reserve = RESERVES / (CLAIM + RESERVES);
Tx_Claims_Paid = 1 - Tx_Claims_Reserve;
Run;

Proc SQL; Create Table Charge_BGD_FC_2_&Country. As
Select a.*, (Case When b.Tx_Claims_Reserve = . Then 0.5 else b.Tx_Claims_Reserve end) As Tx_Claims_Reserve,
		(Case When b.Tx_Claims_Paid = . Then 0.5 else b.Tx_Claims_Paid end) As Tx_Claims_Paid
From Charge_BGD_FC_&Country. a Left Join REPART_CHARGE_&Country._3 b
On (a.country=b.Country and a.SCHEME=b.SCHEME and a.cover_name=b.cover_name);
Quit;

Data TIA.Charge_BGD_FC_2_&Country._net; 
Set Charge_BGD_FC_2_&Country.; 
run;


Data TIA.RESERVES_BGD_FC_&Country._net; 
Set Charge_BGD_FC_2_&Country.;
NET_AMT_2 = NET_AMT * Tx_Claims_Reserve;
attrib POSTE2 length=$ 20;
If POSTE In ('CLAIM') Then POSTE2 = ('RESERVES_CLOT'); /* Warning : splitt in PTEC/PSAP depend on cover */
Drop NET_AMT Tx_Claims_Reserve Tx_Claims_Paid POSTE GWP_FC;
Rename POSTE2 = POSTE;
Rename NET_AMT_2=NET_AMT;
Run;

Data TIA.CLAIMSPAID_BGD_FC_&Country._net; 
Set Charge_BGD_FC_2_&Country.;
NET_AMT_2 = NET_AMT * Tx_Claims_Paid;
Drop NET_AMT Tx_Claims_Reserve Tx_Claims_Paid GWP_FC;
Rename NET_AMT_2=NET_AMT;
Run;



