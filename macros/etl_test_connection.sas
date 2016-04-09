/***************************************************************************************/
/** Usage: checks existence of given tables and generates return code in a predefined 
           macro variable                                                               
	
	"returns" 	0 if all tables exist
				1 if at least one table doesn't exist
				nothing if a mandatory parameter was omitted
*/
/***************************************************************************************/
/*
/** Parameters:
		inTables  	MANDATORY 	fully qualified table references separated with blanks

		outResult	MANDATORY	returning macro variable
*/
/** Example:
	%etl_test_connection(
			 inTables=	work.apple 
		                distora.apfel 
		                distmss.pomme
			,outResult=	tmpallexist
		);
	%put tmpallexist=|&tmpallexist.|;
*/
/** Checks:
	all mandatory parameters have been provided
*/

%macro etl_test_connection(
			 inTables=
			,outResult=
		);

%local 	tmpcnt
		tmptable
		tmptablenum
		;

/** check parameters */
%if &inTables= or &outResult= %then %do;
	%put etl_test_connection :: missing parameter inTables=|&inTables.|, outResult=|&outResult.|;
	%goto exitmacro;
%end; 

/** separate tables */
data _null_;
	length tmptables $32000;
	tmptables = trim(compbl("&inTables.")) || " ";
	length tbl $50 i 8;
	i=1;
	tbl = scan(tmptables,i, " ");
	do while(tbl ne '');
		call symput ( "tmptable"||compress(put(i,best6.-l)) , compress(tbl) );
		i+1;
		tbl = scan(tmptables,i, " ");
	end;
	i = i-1;
	call symput ("tmptablenum", compress(put(i,best6.-l)));
run;

/** check their existence */
%let &outResult.=0;
%do tmpcnt=1 %to &tmptablenum;
	%let &outResult.=%eval( &&&outResult + %eval( 1 - %sysfunc(exist(&&tmptable&tmpcnt)) ) );
	%if %sysfunc(exist(&&tmptable&tmpcnt)) = 0 %then %do;
		%put MISSING TABLE: &&tmptable&tmpcnt;
	%end;
%end;

/** reverse result in order to comply with "normal" boolean vars, i.e. FALSE=0, TRUE>0 */
%if &&&outResult=0 %then %let &outResult.=1;  /* all tables exist */
%else %let &outResult.=0;                     /* at least one table does not exist */

%exitmacro:
%mend etl_test_connection;



/* Test cases
Case01: missing parameters
	%etl_test_connection();

	Result: etl_test_connection :: missing parameter inTables=||, outResult=||

Case02: only inTables filled
	proc datasets nolist nodetails library=work; delete apple; run;
	%etl_test_connection(inTables=work.apple);

	Result: etl_test_connection :: missing parameter inTables=|work.apple|, outResult=||

Case03: table not exists and outResult defined
	proc datasets nolist nodetails library=work; delete apple; run;
	%let tmpallexists=;
	%etl_test_connection(inTables=work.apple, outResult=tmpallexists);
	%put tmpallexists=|&tmpallexists.|;

	Result: tmpallexists=|0|

Case04: table exists and outResult defined
	data apple; length x 8; x=1; run;
	%let tmpallexists=;
	%etl_test_connection(inTables=work.apple, outResult=tmpallexists);
	%put tmpallexists=|&tmpallexists.|;

	Result: tmpallexists=|1|

Case05: many missing tables and outResult defined
	proc datasets nolist nodetails library=work; delete t1 t2 t3 t4; run;
	%let tmpallexists=;
	%etl_test_connection(inTables=work.t1 work.t2 work.t3 work.t4, outResult=tmpallexists);
	%put tmpallexists=|&tmpallexists.|;

	Result: tmpallexists=|0|

Case06: missing and non-missing tables and outResult defined
	data apple; length x 8; x=1; run;
	data apfel; length x 8; x=2; run;
	proc datasets nolist nodetails library=work; delete t1 t2 t3 t4; run;
	%let tmpallexists=;
	%etl_test_connection(inTables=work.t1 work.apple work.t3 work.apfel, outResult=tmpallexists);
	%put tmpallexists=|&tmpallexists.|;

	Result: tmpallexists=|0|

*/

