/*####################################################*/
/*################### INVENTAIRE TIA #################*/
/*####################################################*/

/*#########################################################################*/
/*################### 2ème Etape: Extraction des données  #################*/
/*#########################################################################*/


%let LReseau = X; /* Lettre du serveur "Inventprev" attention au majuscule et minuscule*/
%LET Arrete = 2026_09_Q4;
/*********************************************************************************/

LIBNAME TIA "~/NAS/X/08.Progammes/INTERNATIONAL/06_Inventaire CLP/&Arrete./02_Elements_Techniques/TIA/Extraction Donnees/TIA";

/*------------------------------------------------------------------*/
*==Nouveau libname à condition d'être dans le groupe Unix gg_sas_db_clp ==;
/*----------------------------------------------------------------*/
/**** MODOP accès au datalake CLP via WPS AXA France ****/ 
/*---------------------------------------------------------------*/
* 1/ Vérifier que vous êtes bien connecté au hub :
- WPS > Hub > Connexion
- Mettre son matricule Sxxxxxx 
- Mettre son mdp habituel de connexion à WPS ;

* 2/ Lancé la nouvelle libname ci-dessous :
- nom_libname => à changer selon le nom de la libname souhaité
- nom_db => ne pas changer 
- nom_schema => ne pas changer 
- nom_options => ne pas changer ;

/**************************************************************/


/******* data lake CLP*********/
    %wps_mac_connexion_db(
    nom_libname = clp_wps ,
    nom_db = WPS_SHINE_BLCL ,
    nom_schema = clp_wps ,
    nom_options = readbuff=10000 schema=clp_wps
    );

LIBNAME clp_wps odbcold  DSN=WPS_SHINE_BLCL  authdomain="DB_WPS_SHINE_BLCL"  schema=clp_wps ;    
    
    
data TIA.Daap_level_1_dueonly ;
set  CLP_WPS.daap_level_1_dueonly;
run ;


data TIA.idcf_fr_ugip_cl;
set CLP_WPS.idcf_fr_ugip_cl ;
run ;

