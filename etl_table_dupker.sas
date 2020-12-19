*/------------------------------------------------------------------------*
| Usage: identifies duplicate rows (according to a given set of 		  |
|            KEY columns) in a table                                      |
| 		                                                                  |
| Parameters:                                                             |
| 	inTblToCheck  	MANDATORY 	table to be checked                       |
| 	inKeyCols		MANDATORY	key kolumns enumerated separated by spaces|
| 	inOutResults	OPTIONAL	output table to store duplicate records   |
|                               default: empty => output directed to ODS  |
|                                                                         |
| Example: delete all records (truncate)                                  |
| 	%etl_table_dupker(                                                    |
| 			 inTblToCheck=FUNC.CONNECT_IDS_HIST                           |
| 			,inKeyCols=SYM_DATE DEALID                                    |
| 			,inOutResults=WORK.CONNIDS_DUPS                               |
| 		);                                                                |
|                                                                         |
| Checks:                                                                 |
| 	all mandatory parameters have been provided                           |
| 	table (to be checked) exists                                          |
--------------------------------------------------------------------------*/
%macro etl_table_dupker(
			 inTblToCheck=
			,inKeyCols=
			,inOutResults=
		);

%local	tmpallexist
		tmpkeycols
		tmpjoineq
		tmpcol
		;

%if "&inTblToCheck."="" %then %do;
	%put ERROR: table_dupker :: incoming parameter inTblToCheck is empty! Usage: etl_table_dupker(inTblToCheck=,inKeyCols=,inOutResults=)  -  exit macro;
	%goto exitmacro;
%end;
%if "&inKeyCols."="" %then %do;
	%put ERROR: table_dupker :: incoming parameter inKeyCols is empty! Usage: etl_table_dupker(inTblToCheck=,inKeyCols=,inOutResults=)  -  exit macro;
	%goto exitmacro;
%end;
%if "&inOutResults."="" %then %do;
	%put table_dupker :: incoming parameter inOutResults is empty => duplicates sent to ODS;
%end;

/* Check if table to check exists at all */
%let tmpallexist=;
%etl_test_connection( &inTblToCheck., tmpallexist);
%if ^(&tmpallexist) %then %do;
	%put ERROR: connection to table {&inTblToCheck.} could not be established! exit macro;
	%goto exitmacro;
%end;

/* Check if table to check contains provided key columns */
data _null_;
	set &inTblToCheck.(obs=1 keep=&inKeyCols.);
	delete;
run;
%if(&syserr.>6) %then %do;
	%put ERROR: table_dupker :: table &inTblToCheck. does not contain all of key columns {&inKeyCols.}! exit macro;
	%goto exitmacro;
%end;

/* transform inKeyCols to comma-separated enumeration */
%let tmpkeycols=;
%let tmpkeycols2=;
data _null_;
	length tmpkeycols $10000 tmpcol $32 tmpjoineq $20000 idx 8;
	tmpkeycols = translate( compbl("&inKeyCols.") , ',' , ' ' );  /* replace ' ' with ',' */

	tmpjoineq = "(1=1)";
	idx=1;
	tmpcol = scan(tmpkeycols,idx, ",");
	do while(tmpcol ne '');
		tmpjoineq = compbl(tmpjoineq) || " and d." || compress(tmpcol) ||" eq t." || compress(tmpcol);
		idx+1;
		tmpcol = scan(tmpkeycols,idx, ",");
	end;

	call symput( "tmpkeycols"  , compress(tmpkeycols) );
	call symput( "tmpjoineq"  , compbl(tmpjoineq) );
run;
%put table_dupker :: tmpkeycols =|&tmpkeycols.|;
%put table_dupker :: tmpjoineq=|&tmpjoineq.|;

/* perform check */
%if "&inOutResults."="" %then %do; title "table_dupker :: &inTblToCheck. results"; %end;
proc sql;
	%if "&inOutResults."^="" %then %do; create table &inOutResults. as %end;
	select t.*
	from (
		select &tmpkeycols.
		from &inTblToCheck.
		group by &tmpkeycols.
		having count(1) gt 1
	) d
	left join &inTblToCheck. t
		on &tmpjoineq.
	;
quit;

%exitmacro:
%mend etl_table_dupker;


/** TESTS
data work.ttt;
	length k1 $1 k2 8 d 8;

	k1 = 'A'; k2 = 3; d = 1000; output;
	k1 = 'A'; k2 = 3; d = 10000; output;
	k1 = 'B'; k2 = 3; d = 100; output;
	k1 = 'B'; k2 = 4; d = 200; output;

run;

%etl_table_dupker(inTblToCheck=ttt
			     ,inKeyCols=k1 k2
			     ,inOutResults= t_out);

%etl_table_dupker(inTblToCheck=ttt
			     ,inKeyCols=k1 k2);
*/