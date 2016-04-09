/***************************************************************************************/
/** Usage: wrapper for validity range loader (etl_load_sas_simple_vfvt)
*/
/***************************************************************************************/
/** Parameters:
	inSourceLib			MANDATORY	source library

	inSourceTblName		MANDATORY	source table to be loaded/updated

	inTargetLib			MANDATORY	target library

	inTargetTblName		MANDATORY	target table to be loaded/updated	
	
	inKeyCols			MANDATORY	key columns separated by space WITHOUT VALFROMD(T)/VALTOD(T) cols

	inNoKeyCols			MANDATORY	NON-key columns separated by space WITHOUT VALFROMD(T)/VALTOD(T) cols	

	inCloseUnmatchedRecords	MANDATORY	whether validity of old records without an update in the new set should be closed (YES) or not (NO)
										default: NO

	inVFVTDateFormat	MANDATORY	wether VALFROMD or VALFROMDT to be used
									DATE or DATETIME can be accepted
									default: DATE

	inValidDate			OPTIONAL	validity date in inVFVTDateFormat format (MUST be comparable to valfrom-valto cols)
									default: empty
									when left empty then it is calculated from the system date

	inIndexToRestore	OPTIONAL	indices to be restored past MERGE
									default: empty
									example: key pk=(valfromd key)/unique

	outRC				OPTIONAL	name of macro variable containig return code
										potential values: {SUCCESS, ERROR}
									default value: tmpvfvtwrapper
*/

/** Global variables used: none
*/
/** ETL macros used: 
		%etl_load_sas_simple_vfvt
		%etl_test_connection
*/
/** Checks:
	incoming parameters
*/

%macro etl_simple_vfvt_wrapper(
		 inSourceLib=
		,inSourceTblName=
		,inTargetLib=
		,inTargetTblName=
		,inKeyCols=
		,inNoKeyCols=
		,inCloseUnmatchedRecords=NO
		,inVFVTDateFormat=DATE
		,inValidDate=
		,inIndexToRestore=
		,outRC=tmpvfvtwrapper
	);

%local	tmploadvfvt
		etl_load_dt
		tmpsrclist
		tmpallexist
		tmpnokeycol
		tmpnokeycolrep
		tmpnokeycolnum
		tmpcnt
		tmpvalidd
		tmpvaliddtopass
		;

/** set validity date (validFromDx) according to inVFVTDateFormat
*/
%if &inVFVTDateFormat=DATE %then %do;
	%if &inValidDate= %then %let tmpvalidd=%sysfunc(date());
	                  %else %let tmpvalidd=&inValidDate.;
	%let tmpvaliditycols = VALFROMD VALTOD;
	%let tmpvaliditycondition=VALFROMD le &tmpvalidd. le VALTOD;
	%put Validity date (DATE) = %sysfunc(putn(&tmpvalidd.,yymmdd10.));
	%let tmpvaliddtopass=&tmpvalidd.;
%end; %else %do;
	%if &inValidDate= %then %let tmpvalidd=%sysfunc(datetime());
	                  %else %let tmpvalidd=&inValidDate.;
	%let tmpvaliditycols = VALFROMDT VALTODT;
	%let tmpvaliditycondition=VALFROMDT le &tmpvalidd. le VALTODT;
	%put Validity date (DATETIME) = %sysfunc(putn(&tmpvalidd., datetime23.2));
	%let tmpvaliddtopass=&tmpvalidd.;
%end;

/** check existence of data sources and target */
%let tmpsrclist=
		&inSourceLib..&inSourceTblName.
		&inTargetLib..&inTargetTblName.
	;
%let tmpallexist=;
%etl_test_connection( inTables=&tmpsrclist., outResult=tmpallexist);
%if ^(&tmpallexist) %then %do;
	%let G_ETL_JOB_NOTES=ERROR: connection to table {&tmpsrclist.} could not be established! exit macro;
	%let G_ETL_JOB_RC=ERROR;
	%goto exitmacro;
%end;

/** ETL  -- START */
proc datasets lib=work nolist nodetails;
	delete 	TMPWRP_SOURCEROWSET
			TMPWRP_VALIDKEYS
			TMPWRP_ROWSET_TOCLOSE
			TMPWRP_ROWSET_NEW
			TMPWRP_ROWSET_TOMODIFY
			TMPWRP_COLREPLACEMENTS
			;
run;

/* put KEY and NO-KEY columns into separate variables */
data work.TMPWRP_COLREPLACEMENTS/*_null_*/;
	length tmpcols $32000;
	tmpcols = trim(compbl("&inNoKeyCols.")) || " ";
	length col colrep $32 i 8;
	i=1;
	col = scan(tmpcols,i, " ");
	colrep = "Z" || put(md5(compress(upcase(col))),hex32.);
	do while(col ne '');
		call symput ( "tmpnokeycol"||compress(put(i,best6.-l)) , compress(upcase(col)) );
		call symput ( "tmpnokeycolrep"||compress(put(i,best6.-l)) , compress(colrep) );
		i+1;
		put i= col= colrep=;
		col = scan(tmpcols,i, " ");
		colrep = "Z" || put(md5(compress(upcase(col))),hex32.);
	end;
	i = i-1;
	call symput ("tmpnokeycolnum", compress(put(i,best6.-l)));

	keep col colrep;
run;

/* select SOURCE rowset */
proc sort data=&inSourceLib..&inSourceTblName.
          out=work.TMPWRP_SOURCEROWSET
          nodupkey
		  force;
	by &inKeyCols.;
	sysecho "sort work.TMPWRP_SOURCEROWSET";
run;

/* select actually valid rowset */
proc sort data=&inTargetLib..&inTargetTblName.(
					keep=&inKeyCols. &inNoKeyCols. &tmpvaliditycols.
			        where=(&tmpvaliditycondition.)
				)
          out=work.TMPWRP_VALIDKEYS(keep=&inKeyCols. &inNoKeyCols.)
		  force;
	by &inKeyCols.;
	sysecho "sort Target table valid keys";
run;

/* separate TOCLOSE / NEW / TOMODIFY rowsets */
data work.TMPWRP_ROWSET_TOCLOSE(keep=&inKeyCols.)
     work.TMPWRP_ROWSET_NEW(keep=&inKeyCols. &inNoKeyCols.)
	 work.TMPWRP_ROWSET_TOMODIFY(keep=&inKeyCols. &inNoKeyCols.)
	 ;

	sysecho "TOCLOSE / NEW / TOMODIFY";

	merge work.TMPWRP_VALIDKEYS(in=v)
	      work.TMPWRP_SOURCEROWSET(
					in=s
		  			rename=(%do tmpcnt=1 %to &tmpnokeycolnum.;
								&&tmpnokeycol&tmpcnt.=&&tmpnokeycolrep&tmpcnt.
					        %end;
					       )
				)
		  ;
		by &inKeyCols.;


	if(s=0) then do;/* SOURCE does not contain record for the given KEY */
		output work.TMPWRP_ROWSET_TOCLOSE;
	end; else if(s=1) then do;/* SOURCE does contain record for the given KEY */
		if(v=0) then do; /* no VALID record for given key => set values to MOD_ */

			%do tmpcnt=1 %to &tmpnokeycolnum.;
				&&tmpnokeycol&tmpcnt.=&&tmpnokeycolrep&tmpcnt.;
	        %end;

			output work.TMPWRP_ROWSET_NEW;
		end; else if(v=1) then do;/* there is a VALID record for given key => inspect whether it is different from SOURCE */
			if(     &tmpnokeycol1. ne &tmpnokeycolrep1.
					%do tmpcnt=2 %to &tmpnokeycolnum.;
						or &&tmpnokeycol&tmpcnt. ne &&tmpnokeycolrep&tmpcnt.
			        %end;
			      ) then do; /* VALID record is apparently different from SOURCE */
				
					%do tmpcnt=1 %to &tmpnokeycolnum.;
						&&tmpnokeycol&tmpcnt.=&&tmpnokeycolrep&tmpcnt.;
			        %end;

			   	output work.TMPWRP_ROWSET_TOMODIFY;
			end;/* at least one field is different in VALID and SOURCE */
		end;/* both VALID and SOURCE contain the record for the given KEY */
	end;/* SOURCE does not contain record for the given KEY */
run;

/* handle when unmatched records should not be closed */
%if &inCloseUnmatchedRecords=NO %then %do;
	data work.TMPWRP_ROWSET_TOCLOSE;
		set work.TMPWRP_ROWSET_TOCLOSE(
				obs=1
			);
		delete;
	run;
%end;

/** MERGE */
%let tmploadvfvt=;
%etl_load_sas_simple_vfvt(
		 inTargetLib=&inTargetLib.
		,inTargetTblName=&inTargetTblName.
		,inToCloseDs=work.TMPWRP_ROWSET_TOCLOSE
		,inToModifyDs=work.TMPWRP_ROWSET_TOMODIFY
		,inNewDs=work.TMPWRP_ROWSET_NEW
		,inKeyCols=&inKeyCols.
		,inValidDate=&tmpvaliddtopass.
		,inVFVTDateFormat=&inVFVTDateFormat.
		,outRC=tmploadvfvt
	);

%if &tmploadvfvt^=SUCCESS %then %do;
	%put etl_simple_vfvt_wrapper :: Update of &inTargetLib..&inTargetTblName. failed! exit macro;
	%let &outRC.=ERROR;
	%goto exitmacro;
%end;

/* rebuild indices when needed */
%if "&inIndexToRestore"^="" %then %do;
	data &inTargetLib..&inTargetTblName.(index=(&inIndexToRestore.));
		set &inTargetLib..&inTargetTblName.;
	run;
	%put etl_simple_vfvt_wrapper :: indices rebuilt:;
	%put &inIndexToRestore.;
%end;

%let &outRC.=SUCCESS;

%exitmacro:
%mend etl_simple_vfvt_wrapper;

/** Case 01 - with closing unmatched record, inValidDate provided, DATE format
%let etl_upd_d='15may2015'd;
%put etl_upd_d=%sysfunc(putn(&etl_upd_d.,yymmdd10.));

data basetable
     basetable_befupdate
     ;
	length valfromd valtod 8
	       key $3
		   value 8
		   ;

	format valfromd valtod yymmdd10.;

	key = "K1";
	valfromd = '15JAN2000'd; valtod='31JAN2005'd; value=10; output;
	valfromd = '01FEB2005'd; valtod='15JAN2010'd; value=20; output;
	valfromd = '16JAN2010'd; valtod='31DEC7000'd; value=30; output;

	key = "K2";
	valfromd = '15SEP2004'd; valtod='31DEC2004'd; value=100; output;
	valfromd = '01JAN2005'd; valtod='16JAN2010'd; value=200; output;
	valfromd = '17JAN2010'd; valtod='01JAN2012'd; value=300; output;

	key = "K3";
	valfromd = '15SEP2004'd; valtod='31DEC2004'd; value=1000; output;
	valfromd = '01JAN2005'd; valtod='16JAN2010'd; value=2000; output;
	valfromd = '17JAN2010'd; valtod='31DEC7000'd; value=3000; output;
run;

data newset;
	length key $3
		   value 8
		   ;

	key = "K3"; value=50; output;
	key = "K2"; value=500; output;
run;


%etl_simple_vfvt_wrapper(
		 inSourceLib=WORK
		,inSourceTblName=newset
		,inTargetLib=WORK
		,inTargetTblName=basetable
		,inKeyCols=key
		,inNoKeyCols=value
		,inCloseUnmatchedRecords=YES
		,inVFVTDateFormat=DATE
		,inValidDate=&etl_upd_d.
		,inIndexToRestore=key pk=(valfromd key)
		,outRC=tmpvfvtwrapper
	);
*/

/** Case 02 - WITHOUT closing unmatched record, inValidDate provided, DATE format
%let etl_upd_d='15may2015'd;
%put etl_upd_d=%sysfunc(putn(&etl_upd_d.,yymmdd10.));

data basetable
     basetable_befupdate
     ;
	length valfromd valtod 8
	       key $3
		   value 8
		   ;

	format valfromd valtod yymmdd10.;

	key = "K1";
	valfromd = '15JAN2000'd; valtod='31JAN2005'd; value=10; output;
	valfromd = '01FEB2005'd; valtod='15JAN2010'd; value=20; output;
	valfromd = '16JAN2010'd; valtod='31DEC7000'd; value=30; output;

	key = "K2";
	valfromd = '15SEP2004'd; valtod='31DEC2004'd; value=100; output;
	valfromd = '01JAN2005'd; valtod='16JAN2010'd; value=200; output;
	valfromd = '17JAN2010'd; valtod='01JAN2012'd; value=300; output;

	key = "K3";
	valfromd = '15SEP2004'd; valtod='31DEC2004'd; value=1000; output;
	valfromd = '01JAN2005'd; valtod='16JAN2010'd; value=2000; output;
	valfromd = '17JAN2010'd; valtod='31DEC7000'd; value=3000; output;
run;

data newset;
	length key $3
		   value 8
		   ;

	key = "K3"; value=50; output;
	key = "K2"; value=500; output;
run;


%etl_simple_vfvt_wrapper(
		 inSourceLib=WORK
		,inSourceTblName=newset
		,inTargetLib=WORK
		,inTargetTblName=basetable
		,inKeyCols=key
		,inNoKeyCols=value
		,inCloseUnmatchedRecords=NO
		,inVFVTDateFormat=DATE
		,inValidDate=&etl_upd_d.
		,inIndexToRestore=key pk=(valfromd key)
		,outRC=tmpvfvtwrapper
	);
*/

/** Case 03 - WITHOUT closing unmatched record, inValidDate NOT provided, DATE format
data basetable
     basetable_befupdate
     ;
	length valfromd valtod 8
	       key $3
		   value 8
		   ;

	format valfromd valtod yymmdd10.;

	key = "K1";
	valfromd = '15JAN2000'd; valtod='31JAN2005'd; value=10; output;
	valfromd = '01FEB2005'd; valtod='15JAN2010'd; value=20; output;
	valfromd = '16JAN2010'd; valtod='31DEC7000'd; value=30; output;

	key = "K2";
	valfromd = '15SEP2004'd; valtod='31DEC2004'd; value=100; output;
	valfromd = '01JAN2005'd; valtod='16JAN2010'd; value=200; output;
	valfromd = '17JAN2010'd; valtod='01JAN2012'd; value=300; output;

	key = "K3";
	valfromd = '15SEP2004'd; valtod='31DEC2004'd; value=1000; output;
	valfromd = '01JAN2005'd; valtod='16JAN2010'd; value=2000; output;
	valfromd = '17JAN2010'd; valtod='31DEC7000'd; value=3000; output;
run;

data newset;
	length key $3
		   value 8
		   ;

	key = "K3"; value=50; output;
	key = "K2"; value=500; output;
run;


%etl_simple_vfvt_wrapper(
		 inSourceLib=WORK
		,inSourceTblName=newset
		,inTargetLib=WORK
		,inTargetTblName=basetable
		,inKeyCols=key
		,inNoKeyCols=value
		,inCloseUnmatchedRecords=NO
		,inVFVTDateFormat=DATE
		,inValidDate=
		,inIndexToRestore=key pk=(valfromd key)
		,outRC=tmpvfvtwrapper
	);
*/

/** Case 04 - WITHOUT closing unmatched record, inValidDate NOT provided, DATETIME format
data basetable
     basetable_befupdate
     ;
	length valfromdt valtodt 8
	       key $3
		   value 8
		   ;

	format valfromdt valtodt yymmdd10.;

	key = "K1";
	valfromdt = '15JAN2000'd; valtodt='31JAN2005'd; value=10; output;
	valfromdt = '01FEB2005'd; valtodt='15JAN2010'd; value=20; output;
	valfromdt = '16JAN2010'd; valtodt='31DEC7000'd; value=30; output;

	key = "K2";
	valfromdt = '15SEP2004'd; valtodt='31DEC2004'd; value=100; output;
	valfromdt = '01JAN2005'd; valtodt='16JAN2010'd; value=200; output;
	valfromdt = '17JAN2010'd; valtodt='01JAN2012'd; value=300; output;

	key = "K3";
	valfromdt = '15SEP2004'd; valtodt='31DEC2004'd; value=1000; output;
	valfromdt = '01JAN2005'd; valtodt='16JAN2010'd; value=2000; output;
	valfromdt = '17JAN2010'd; valtodt='31DEC7000'd; value=3000; output;
run;

data newset;
	length key $3
		   value 8
		   ;

	key = "K3"; value=50; output;
	key = "K2"; value=500; output;
run;


%etl_simple_vfvt_wrapper(
		 inSourceLib=WORK
		,inSourceTblName=newset
		,inTargetLib=WORK
		,inTargetTblName=basetable
		,inKeyCols=key
		,inNoKeyCols=value
		,inCloseUnmatchedRecords=NO
		,inVFVTDateFormat=DATETIME
		,inValidDate=
		,inIndexToRestore=key pk=(valfromdt key)
		,outRC=tmpvfvtwrapper
	);
*/