
%let LReseau =  ~/NAS/X ; /* Mettre le serveur approprié  entre -> ~/NAS/X  ou -> X:/Inventprev ** */
%let Arrete = 2026_09_Q4 ;

LIBNAME TIA "&LReseau./08.Progammes/INTERNATIONAL/06_Inventaire CLP/&Arrete./02_Elements_Techniques/TIA/Extraction Donnees/TIA";

%macro resize(lib=work, mem=, out=&mem.);

    proc sql noprint;
        /* We get the list of character columns */
        SELECT NAME
        INTO :CHARCOLS SEPARATED BY ' '
        FROM dictionary.columns
        WHERE LIBNAME=upcase("&lib.") AND MEMNAME=upcase("&mem.")
        AND upcase(TYPE)="CHAR";
 
        /* For each char column we get the minimum length that avoids truncation */
        /* EX:/Inventprev "var1 $5. var2 $7. ..."                                           */
        %let nchars=%sysfunc(countw(&CHARCOLS));
 
        %do i=1 %to &nchars.;
          SELECT cat("length ",
            %let col=%scan(&CHARCOLS.,&i.,' ');
            " &col. $",max(length(&col.)),".;", " format &col $",max(length(&col.)),".;") 
            INTO :lengths&i SEPARATED BY ' ' FROM &lib..&mem.;
        %end;
    quit;

    /* New dataset with resized char columns */
    data &out.;
       %do i=1 %to &nchars.;
        &&lengths&i.
       %end;
        set &lib..&mem.;
    run;

    /* remove the informats on character variables as the data already exists */
    %let outlib=%upcase(%scan(&out,1,'.'));
    %let outmem=%upcase(%scan(&out,2,'.'));
    %if  &outmem=%str() %then %do;
      %let outmem=&outlib;
      %let outlib=work;
    %end;
    proc datasets lib=&outlib memtype=data nolist;
    modify &outmem;
       attrib _char_ informat=;
   run;
    
%mend resize;

 
%macro Split_Country (Country=) ; 
Data TIA.GLOBAL_PL_&Country. ;
set TIA.DAAP_LEVEL_1_DUEONLY ;
where countryid_vorig = "&Country." ;
run;
/*%resize(lib=TIA, mem=GLOBAL_PL_&Country., out=TIA.GLOBAL_PL_&Country.);*/
%mend ;


%Split_Country(Country=PE); 
%Split_Country(Country=LU); 
%Split_Country(Country=AT); 
%Split_Country(Country=BE); 
%Split_Country(Country=CH);
%Split_Country(Country=CO); 
%Split_Country(Country=DE);
%Split_Country(Country=DK);
%Split_Country(Country=FI);
%Split_Country(Country=FR);
%Split_Country(Country=GR);
%Split_Country(Country=IE);
%Split_Country(Country=MX);
%Split_Country(Country=NI);
%Split_Country(Country=NL);
%Split_Country(Country=NO);
%Split_Country(Country=PL);
%Split_Country(Country=PT);
%Split_Country(Country=SE);
%Split_Country(Country=TR);
%Split_Country(Country=UK);
%Split_Country(Country=LT); 
%Split_Country(Country=ES);
%Split_Country(Country=IT);

