/***************************************************************************************/
/** Usage: loads into the target table by setting record validity properties
	WARNING! re-creates the target table but doesn't re-creates indices!
	Supposes that 
		- fields indicating the validity range borders exist in Target table
		- their name is {VALFROMD, VALTOD} or {VALFROMDT, VALTODT}
*/
/***************************************************************************************/
/*
/** Parameters:
	inTargetLib			MANDATORY	target library

	inTargetTblName		MANDATORY	target table to be loaded/updated

	inToCloseDs			CONDITIONAL	dataset containing records to be closed
										when left empty: no records' validity would be closed

	inToModifyDs		CONDITIONAL	dataset containing records to be modified
										when left empty: no records would be modified 
											(i.e. validity closed and new record with open validity range added)

	inNewDs				CONDITIONAL	dataset containing new records		
										when left empty: no new records will be added

	inVFVTDateFormat	MANDATORY	DATE or DATETIME can be accepted. 
										DATE => {VALFROMD, VALTOD} supposed
										DATETIME => {VALFROMDT, VALTODT} supposed
										default: DATE

	inKeyCols			MANDATORY	natural key columns separated by space WITHOUT VALFROMD(T)/VALTOD(T) cols

	inValidDate			OPTIONAL	validity date in inVFVTDateFormat format (MUST be comparable to VALFROMD(T)-VALTOD(T) cols)
									default: empty
									when left empty then it is calculated from the system date

	outRC				OPTIONAL	name of macro variable containig return code
										potential values: {SUCCESS, ERROR}
*/
/** Uses:
	%etl_test_connection
*/
/** Checks:
	all mandatory variables are present
	source and target datasets exist
	target dataset and explicitly provided source datasets contain KEY columns
*/

%macro etl_load_sas_simple_vfvt(
		 inTargetLib=
		,inTargetTblName=
		,inToCloseDs=
		,inToModifyDs=
		,inNewDs=
		,inKeyCols=
		,inVFVTDateFormat=DATE
		,inValidDate=
		,outRC=
	);

%local	tmpvalfrom
		tmpvalto
		tmpvaltoinfinity
		tmpvalfromcol
		tmpvaltocol
		t1
		t2
		tmpdsid
		tmprc
		tmpvarname
		tmpnewcol
		tmpkeycol
		tmpkeycolnum
		tmpnokeycol
		tmpnokeycolnum
		tmpnokeycollist
		tmprecnumnew
		tmprecnumclosed
		tmprecnummodified
		tmprecnumexc1
		tmprecnumexc2
		tmprecnumexc3
		tmpallexist
		tmpsrclist
		tmpchkrc
		tmpvaliddate
		tmpoutrc;
		;

/** print incoming params */
%put MACRO etl_load_sas_simple_vfvt STARTED;
%put Incoming paramters:;
%put inTargetTblName=|&inTargetTblName.|;
%put inToCloseDs=|&inToCloseDs.|;
%put inToModifyDs=|&inToModifyDs.|;
%put inNewDs=|&inNewDs.|;
%put inKeyCols=|&inKeyCols.|;
%put inValidDate=|&inValidDate.|;
/*%put inLoadDateColName=|&inLoadDateColName.|;*/
%put inVFVTDateFormat=|&inVFVTDateFormat.|;
%put outRC=|&outRC.|;

/** check incoming MANDATORY and CONDITIONAL params */
%if &inTargetLib= or &inTargetTblName= or &inKeyCols= or &inVFVTDateFormat= %then %do;
	%put ERROR: Mandatory parameter is set to NULL! exit macro;
	%let tmpoutrc=ERROR;
	%goto exitmacro;
%end;
%if &inToCloseDs= and &inToModifyDs= and &inNewDs= %then %do;
	%put WARNING: macro called with no sources specified! skip loading;
	%goto exitmacro;
%end; 
%let inVFVTDateFormat=%upcase(&inVFVTDateFormat.);
%if &inVFVTDateFormat^=DATE and &inVFVTDateFormat^=DATETIME %then %do;
	%put ERROR: inVFVTDateFormat can only be set to DATE or DATETIME! exit macro;
	%let tmpoutrc=ERROR;
	%goto exitmacro;
%end;

/** check existence of target table and provided source tables */
%let tmpallexist=;
%let tmpsrclist=&inTargetLib..&inTargetTblName.;
%if &inNewDs.^= %then %let tmpsrclist=&tmpsrclist &inNewDs.;
%if &inToCloseDs.^= %then %let tmpsrclist=&tmpsrclist &inToCloseDs.;
%if &inToModifyDs.^= %then %let tmpsrclist=&tmpsrclist &inToModifyDs.;
%etl_test_connection( inTables=&tmpsrclist. , outResult=tmpallexist);
%if ^(&tmpallexist) %then %do;
	%put ERROR: connection to table {&tmpsrclist.} could not be established! exit macro;
	%let tmpoutrc=ERROR;
	%goto exitmacro;
%end;

/** check whether KEY columns are present in Source and TARGET tables */
	/** macro to check it */
	%macro tmpchkkeycolsintable(chkInDs,chkInKeyCols,chkOutRC);
		%let &chkOutRC=0;
		data _null_;
			set &chkInDs.(obs=1 keep=&chkInKeyCols.);
		run;
		%if(&syserr.>6) %then %do;
			%put ERROR: Source dataset &chkInDs. does not contain all KEY variables! exit macro;
			%put SYSERRORTEXT=|&SYSERRORTEXT.|;
			%let &chkOutRC=1;
		%end;
	%mend tmpchkkeycolsintable;

%let tmpallexist=0;
/* check target table first */
%tmpchkkeycolsintable(&inTargetLib..&inTargetTblName.,&inKeyCols.,tmpchkrc);
%let tmpallexist=%eval(&tmpallexist. + &tmpchkrc.);
/* create data sources that haven't been explicitly provided */
%if &inNewDs.= %then %do;
	data work.qwertzqwertzinnewds;
		set &inTargetLib..&inTargetTblName.(obs=1 keep=&inKeyCols.);
		delete;
	run;
	%if(&syserr.<=6) %then %do;
		%let inNewDs=&syslast;
		%put Param inNewDs left empty so an empty table &syslast has been created with only the KEY cols.;
	%end;
%end;
%if &inToCloseDs.= %then %do;
	data work.qwertzqwertzintocloseds;
		set &inTargetLib..&inTargetTblName.(obs=1 keep=&inKeyCols.);
		delete;
	run;
	%if(&syserr.<=6) %then %do;
		%let inToCloseDs=&syslast;
		%put Param inToCloseDs left empty so an empty table &syslast has been created with only the KEY cols.;
	%end;
%end;
%if &inToModifyDs.= %then %do;
	data work.qwertzqwertzintomodifyds;
		set &inTargetLib..&inTargetTblName.(obs=1 keep=&inKeyCols.);
		delete;
	run;
	%if(&syserr.<=6) %then %do;
		%let inToModifyDs=&syslast;
		%put Param inToModifyDs left empty so an empty table &syslast has been created with only the KEY cols.;
	%end;
%end;
/* go on with checking */
%tmpchkkeycolsintable(&inNewDs.,&inKeyCols.,tmpchkrc);
%let tmpallexist=%eval(&tmpallexist. + &tmpchkrc.);
%tmpchkkeycolsintable(&inToCloseDs.,&inKeyCols.,tmpchkrc);
%let tmpallexist=%eval(&tmpallexist. + &tmpchkrc.);
%tmpchkkeycolsintable(&inToModifyDs.,&inKeyCols.,tmpchkrc);
%let tmpallexist=%eval(&tmpallexist. + &tmpchkrc.);
%if &tmpallexist > 0 %then %do;
	%put ERROR: KEY columns are NOT present in all source and target tables! exit macro;
	%let tmpoutrc=ERROR;
	%goto exitmacro;
%end;

/** calc local vars -- START */
/** set valfrom and valto according to the given format */
%if &inVFVTDateFormat=DATE %then %do;
	%if &inValidDate^= %then %let tmpvaliddate=&inValidDate.;
	                   %else %let tmpvaliddate=%sysfunc(date());
	%let tmpvalfrom=%sysfunc(compress(%sysfunc(putn(&tmpvaliddate.,best32.))));
	%let tmpvalfromcol=valfromd;
	%let tmpvaltocol=valtod;
	%let tmpvaltoinfinity=%sysfunc(compress(%sysfunc(putn('31dec7000'd,best32.-l))));
%end; %else %do;
	%if &inValidDate^= %then %let tmpvaliddate=&inValidDate.;
	                   %else %let tmpvaliddate=%sysfunc(datetime());
	%let tmpvalfrom=%sysfunc(ceil(&tmpvaliddate.));
	%let tmpvalfromcol=valfromdt;
	%let tmpvaltocol=valtodt;
	%let tmpvaltoinfinity=%sysfunc(compress(%sysfunc(putn('31dec7000:0:0:0'dt,best32.-l))));
%end;
%let tmpvalto=%sysevalf(&tmpvalfrom. - 1);
/** set separate key columns */
data _null_;
	length tmpcols $32000;
	tmpcols = trim(compbl("&inKeyCols.")) || " ";
	length col $32 i 8;
	i=1;
	col = scan(tmpcols,i, " ");
	do while(col ne '');
		call symput ( "tmpkeycol"||compress(put(i,best6.-l)) , compress(upcase(col)) );
		i+1;
		col = scan(tmpcols,i, " ");
	end;
	i = i-1;
	call symput ("tmpkeycolnum", compress(put(i,best6.-l)));
run;
/** set separate NO-KEY columns */
%let tmpdsid=%sysfunc(open(&inToModifyDs.,i));
%let tmpnokeycolnum=0;
%let tmpnokeycollist=;
%do t1=1 %to %sysfunc(attrn(&tmpdsid.,nvars));
	%let tmpvarname=%upcase(%sysfunc(varname(&tmpdsid., &t1.)));
	/* is it a technical column? */
	%if /*&tmpvarname^=&inLoadDateColName. and*/ &tmpvarname^=&tmpvalfromcol. and &tmpvarname^=&tmpvaltocol. %then %do;
		%let tmpnewcol=0;
		%do t2=1 %to &tmpkeycolnum.; /* is it a KEY column? */
			%if &tmpvarname=&&tmpkeycol&t2. %then %do;
				%let tmpnewcol=1; /* this is a key column indeed */
			%end;
		%end; /* next t2 */
		%if &tmpnewcol=0 %then %do; /* the column is definitely not a KEY column so let's add it to the list */
			%let tmpnokeycolnum=%eval( &tmpnokeycolnum. + 1 );
			%let tmpnokeycol&tmpnokeycolnum.=&tmpvarname.;
			%let tmpnokeycollist=&tmpnokeycollist. &tmpvarname.;
		%end;
	%end;/* surely not a technical column */
%end; /* next t1 */
%let tmprc=%sysfunc(close(&tmpdsid.));
%if &tmpnokeycollist= %then %do;
	%put NO-KEY columns not found;
%end; %else %do;
	%put NO-KEY columns identified: &tmpnokeycollist.;
%end;
/** calc local vars -- END */


/** ETL -- START */
/** sort everything */
proc sort data=&inToCloseDs.
          force; 
	by &inKeyCols.;
run;
proc sort data=&inToModifyDs.
          force; 
	by &inKeyCols.;
run;
proc sort data=&inTargetLib..&inTargetTblName.
          force; 
	by &inKeyCols. &tmpvalfromcol.;
run;

/** close & modify */
proc datasets lib=work nolist nodetails;
	delete 	tmpexcesstoclose 
			tmpexcesstomodify 
			tmpcloseandmodify
			tmptargettable
			;
run;
%let tmprecnumnew=0;
%let tmprecnumclosed=0;
%let tmprecnummodified=0;
%let tmprecnumexc1=0;
%let tmprecnumexc2=0;
%let tmprecnumexc3=0;
data work.tmptargettable(keep=	&tmpvalfromcol. 
								&tmpvaltocol. 
								&inKeyCols. 
								&tmpnokeycollist.)
     work.tmpexcesstoclose(drop=ztrewqtmprecnumclosed ztrewqtmprecnummodified ztrewqtmprecnumexc1 ztrewqtmprecnumexc2 ztrewqtmprecnumexc3)
	 work.tmpexcesstomodify(drop=ztrewqtmprecnumclosed ztrewqtmprecnummodified ztrewqtmprecnumexc1 ztrewqtmprecnumexc2 ztrewqtmprecnumexc3)
	 work.tmpcloseandmodify(drop=ztrewqtmprecnumclosed ztrewqtmprecnummodified ztrewqtmprecnumexc1 ztrewqtmprecnumexc2 ztrewqtmprecnumexc3)
     ;

	length ztrewqtmprecnumclosed ztrewqtmprecnummodified ztrewqtmprecnumexc1 ztrewqtmprecnumexc2 ztrewqtmprecnumexc3 8;
	retain ztrewqtmprecnumclosed 0 ztrewqtmprecnummodified 0 ztrewqtmprecnumexc1 0 ztrewqtmprecnumexc2 0 ztrewqtmprecnumexc3 0;

	merge &inTargetLib..&inTargetTblName.(in=v)
	      &inToCloseDs.(in=c keep=&inKeyCols. )
		  &inToModifyDs.(in=m
		                 keep=&inKeyCols. &tmpnokeycollist.
		                 %if &tmpnokeycolnum^=0 %then %do; /* NO-KEY columns to be renamed! */
		                     rename=(
							 	%do t1=1 %to &tmpnokeycolnum.;
									&&tmpnokeycol&t1=qwertzqwertz&t1
								%end; /* next t1 */
		                            )
						 %end;/* rename finished */
	                     )
			end=vege
	      ;
		by &inKeyCols.;

	*put v= c= m= _all_;

	if(v=1 and c=1 and m=0) then do;
		if(&tmpvalfromcol. le &tmpvaliddate. le &tmpvaltocol.) then do;
			/** close valto and output */
			&tmpvaltocol. = &tmpvalto.;
			output work.tmptargettable;
			ztrewqtmprecnumclosed = ztrewqtmprecnumclosed + 1;
		end; else do;
			/** record isn't valid anymore so output it without change */
			output work.tmptargettable;
		end;
	end; else if(v=1 and m=1) then do;
		if(&tmpvalfromcol. le &tmpvaliddate. le &tmpvaltocol.) then do;
			/** close the modified record */
			&tmpvaltocol. = &tmpvalto.;
			output work.tmptargettable;
			*put "close the modified record " &tmpvalfromcol.= &tmpvaltocol.=;

			/** add the new one with validity left open */
			%if &tmpnokeycolnum^=0 %then %do; /* overwrite NO-KEY columns, if any, from the modifying set */
				%do t1=1 %to &tmpnokeycolnum.;
					&&tmpnokeycol&t1=qwertzqwertz&t1;
					*put "&&tmpnokeycol&t1 overwritten";
				%end; /* next t1 */ 
			%end;/* overwrite finished */
			&tmpvalfromcol. = &tmpvalfrom.;
			&tmpvaltocol. = &tmpvaltoinfinity.;
			output work.tmptargettable;
			*put "add the new one with validity left open " &tmpvalfromcol.= &tmpvaltocol.=;

			ztrewqtmprecnummodified = ztrewqtmprecnummodified + 1;

			/** anyway, if the record was labeled both as to be closed and modified
				then a soft warning should be issued
			*/
			if(c=1 and m=1) then do;
				output work.tmpcloseandmodify;
				ztrewqtmprecnumexc3 = ztrewqtmprecnumexc3 + 1;
				*put "the record was labeled both as to be closed and modified " &tmpvalfromcol.= &tmpvaltocol.=;
			end;
		end; else do;
			/** already invalid records appearing on a MODIFY list */
			output work.tmptargettable;
			*put "already invalid records appearing on a MODIFY list " &tmpvalfromcol.= &tmpvaltocol.=;
		end;

	end; else if(v=1 and m=0 and c=0) then do;
		/** records not affected at all */
		output work.tmptargettable;
		*put "records not affected at all";
	end; else if(v=0 and m=1 and c=0) then do;
		/** record to be closed is not valid! */
		output work.tmpexcesstoclose;
		ztrewqtmprecnumexc1 = ztrewqtmprecnumexc1 + 1;
		*put "record to be closed is not valid!";

	end; else if(v=0 and c=1 and m=0) then do;
		/** record to be modified is not valid! */
		output work.tmpexcesstomodify;
		ztrewqtmprecnumexc2 = ztrewqtmprecnumexc2 + 1;
		*put "record to be modified is not valid!";

	end;

	/** save record numbers when finished */
	if(vege) then do;
		call symput("tmprecnumclosed", compress(put(ztrewqtmprecnumclosed, best32.-l)));
		call symput("tmprecnummodified", compress(put(ztrewqtmprecnummodified, best32.-l)));
		call symput("tmprecnumexc1", compress(put(ztrewqtmprecnumexc1, best32.-l)));
		call symput("tmprecnumexc2", compress(put(ztrewqtmprecnumexc2, best32.-l)));
		call symput("tmprecnumexc3", compress(put(ztrewqtmprecnumexc3, best32.-l)));
	end;

run;
/** check whether update was succesful */
%if(&syserr.>6) %then %do;
	%put ERROR: Update of &inTargetLib..&inTargetTblName. failed! exit macro;
	%put SYSERRORTEXT=|&SYSERRORTEXT.|;
	%let tmpoutrc=ERROR;
	%goto exitmacro;
%end; %else %do;
	/** check whether any record found in excess */
	%put Update of &inTargetLib..&inTargetTblName. finished;
	%put Records closed=&tmprecnumclosed. , modified=&tmprecnummodified.;
	%if &tmprecnumexc1^=0 or &tmprecnumexc2^=0 or &tmprecnumexc3^=0 %then %do;
		%put Input datasets were overlapping:;
		%put Keys intended to be modified but never existed=&tmprecnumexc1.;
		%put Records intended to be closed but never existed=&tmprecnumexc2.;
		%put Records intended to be both closed and modified=&tmprecnumexc3.;
	%end;
%end;

/** append new records */
data &inNewDs.(keep=/*&inLoadDateColName.*/ &tmpvalfromcol. &tmpvaltocol. &inKeyCols. &tmpnokeycollist.);
	set &inNewDs.(keep=&inKeyCols. &tmpnokeycollist.)
	    end=vege;

	length &tmpvalfromcol. &tmpvaltocol. 8;
	&tmpvalfromcol. = &tmpvalfrom.;
	&tmpvaltocol. = &tmpvaltoinfinity.;

	length ztrewqtmprecnumnew 8;
	retain ztrewqtmprecnumnew 0;
	ztrewqtmprecnumnew = ztrewqtmprecnumnew + 1;

	if(vege) then do;
		call symput("tmprecnumnew", compress(put(ztrewqtmprecnumnew, best32.-l)));
	end;
run;
proc append base=work.tmptargettable
            data=&inNewDs.
			force
			;
run;
%if(&syserr.>6) %then %do;
	%put ERROR: Append new records to work.tmptargettable failed! exit macro;
	%let tmpoutrc=ERROR;
	%put SYSERRORTEXT=|&SYSERRORTEXT.|;
	%goto exitmacro;
%end; %else %do;
	%put Append new records to work.tmptargettable &inTargetLib..&inTargetTblName. finished;
	%put Records added=&tmprecnumnew.;
%end;
/** replace target DS if all operations succeeded so far */
proc datasets lib=&inTargetLib. nolist nodetails;
	delete &inTargetTblName.;
run;
data &inTargetLib..&inTargetTblName.;
	set work.tmptargettable;
run;
%if(&syserr.>6) %then %do;
	%put ERROR: Replacement of &inTargetLib..&inTargetTblName. failed! exit macro;
	%let tmpoutrc=ERROR;
	%put SYSERRORTEXT=|&SYSERRORTEXT.|;
	%goto exitmacro;
%end; %else %do;
	%put Replacement of &inTargetLib..&inTargetTblName. succeeded;
	%put Records added=&tmprecnumnew.;
	%let tmpoutrc=SUCCESS;
%end;
/** ETL -- END */
%exitmacro:
%if &outRC^= %then %do;
	%if %symexist(&outRC) %then %do;
		%let &outRC=&tmpoutrc.;
	%end;
%end;
%put MACRO etl_load_sas_simple_vfvt ENDED;
%mend etl_load_sas_simple_vfvt;

/**TestCases

Case01: missing parameter
%etl_load_sas_simple_vfvt();
Result: ERROR: Mandatory parameter is set to NULL! exit macro

Case02: wrong parameter
%etl_load_sas_simple_vfvt(
		 inTargetLib=WORK
		,inTargetTblName=TESTTARGET
		,inToCloseDs=WORK.TESTTOCLOSE
		,inToModifyDs=WORK.TESTTOMODIFY
		,inNewDs=WORK.TESTNEW
		,inKeyCols=key1 key2
		,inValidDate='30JUN2015'd
		,inVFVTDateFormat=NOTgood
	);
Result: ERROR: inVFVTDateFormat can only be set to DATE or DATETIME! exit macro

Case03: missing source dataset
proc datasets nolist nodetails lib=work; delete TESTTARGET; run;
data WORK.TESTTARGET;
	length nat_key 8;
run;
%etl_load_sas_simple_vfvt(
		 inTargetLib=WORK
		,inTargetTblName=TESTTARGET
		,inToCloseDs=WORK.TESTTOCLOSE
		,inToModifyDs=WORK.TESTTOMODIFY
		,inNewDs=WORK.TESTNEW
		,inKeyCols=key1 key2
		,inValidDate='30JUN2015'd
		,inVFVTDateFormat=DATE
	);
Result: ERROR: connection to table {WORK.TESTNEW WORK.TESTTOCLOSE WORK.TESTTOMODIFY WORK.TESTTARGET} could not be established! exit macro

Case04: missing target dataset
proc datasets nolist nodetails lib=work; delete TESTTARGET TESTTOCLOSE TESTTOMODIFY TESTNEW; run;
data WORK.TESTTOCLOSE; length nat_key 8; run;
data WORK.TESTTOMODIFY; length nat_key 8; run;
data WORK.TESTNEW; length nat_key 8; run;
%etl_load_sas_simple_vfvt(
		 inTargetLib=WORK
		,inTargetTblName=TESTTARGET
		,inToCloseDs=WORK.TESTTOCLOSE
		,inToModifyDs=WORK.TESTTOMODIFY
		,inNewDs=WORK.TESTNEW
		,inKeyCols=key1 key2
		,inValidDate='30JUN2015'd
		,inVFVTDateFormat=DATE
	);
Result: ERROR: connection to table {WORK.TESTNEW WORK.TESTTOCLOSE WORK.TESTTOMODIFY WORK.TESTTARGET} could not be established! exit macro

Case05: natural key element is missing from at least one of the source tables
proc datasets nolist nodetails lib=work; delete TESTTARGET TESTTOCLOSE TESTTOMODIFY TESTNEW; run;
data WORK.TESTTARGET; length key1 key2 8; run;
data WORK.TESTTOCLOSE; length nat_key 8; run;
data WORK.TESTTOMODIFY; length nat_key 8; run;
data WORK.TESTNEW; length nat_key 8; run;
%etl_load_sas_simple_vfvt(
		 inTargetLib=WORK
		,inTargetTblName=TESTTARGET
		,inToCloseDs=WORK.TESTTOCLOSE
		,inToModifyDs=WORK.TESTTOMODIFY
		,inNewDs=WORK.TESTNEW
		,inKeyCols=key1 key2
		,inValidDate='30JUN2015'd
		,inVFVTDateFormat=DATE
	);
Result:
ERROR: The variable key1 in the DROP, KEEP, or RENAME list has never been referenced.
ERROR: The variable key2 in the DROP, KEEP, or RENAME list has never been referenced.
ERROR: Source dataset WORK.TESTTOMODIFY does not contain all KEY variables! exit macro
ERROR: KEY columns are NOT present in all source and target tables! exit macro

Case06: natural key element is missing from the target table
proc datasets nolist nodetails lib=work; delete TESTTARGET TESTTOCLOSE TESTTOMODIFY TESTNEW; run;
data WORK.TESTTARGET; length nat_key 8; run;
data WORK.TESTTOCLOSE; length key1 key2 8; run;
data WORK.TESTTOMODIFY; length key1 key2 8; run;
data WORK.TESTNEW; length key1 key2 8; run;
%etl_load_sas_simple_vfvt(
		 inTargetLib=WORK
		,inTargetTblName=TESTTARGET
		,inToCloseDs=WORK.TESTTOCLOSE
		,inToModifyDs=WORK.TESTTOMODIFY
		,inNewDs=WORK.TESTNEW
		,inKeyCols=key1 key2
		,inValidDate='30JUN2015'd
		,inVFVTDateFormat=DATE
	);
Result:
ERROR: The variable key1 in the DROP, KEEP, or RENAME list has never been referenced.
ERROR: The variable key2 in the DROP, KEEP, or RENAME list has never been referenced.
ERROR: Source dataset WORK.TESTTARGET does not contain all KEY variables! exit macro
ERROR: KEY columns are NOT present in all source and target tables! exit macro

Case07: empty target dataset, type=DATE
proc datasets nolist nodetails lib=work; delete TESTTARGET TESTTOCLOSE TESTTOMODIFY TESTNEW; run;
data WORK.TESTTARGET; 
	length etl_load_dt valfromd valtod key1 key2 8 nokey $30;
	format etl_load_dt datetime20. valfromd valtod yymmdd10.;
	delete; 
run;
data WORK.TESTNEW; 
	length etl_load_dt valfromd valtod key1 key2 8 nokey $30;
	key1=1; key2=1; nokey="old";
run;
data WORK.TESTTOMODIFY; 
	length key1 key2 8 nokey $30;
	delete; 
run;
data WORK.TESTTOCLOSE; 
	length key1 key2 8;
	delete; 
run;
%etl_load_sas_simple_vfvt(
		 inTargetLib=WORK
		,inTargetTblName=TESTTARGET
		,inToCloseDs=WORK.TESTTOCLOSE
		,inToModifyDs=WORK.TESTTOMODIFY
		,inNewDs=WORK.TESTNEW
		,inKeyCols=key1 key2
		,inValidDate='30JUN2015'd
		,inVFVTDateFormat=DATE
	);
Result:
Update of WORK.TESTTARGET finished
Records closed=0 , modified=0
Append new records to WORK.TESTTARGET finished
Records added=1

Case08: overlapping source dataset, type=DATE
proc datasets nolist nodetails lib=work; delete TESTTARGET TESTTOCLOSE TESTTOMODIFY TESTNEW; run;
data WORK.TESTTARGET; 
	length etl_load_dt valfromd valtod key1 key2 8 nokey $30;
	format etl_load_dt datetime20. valfromd valtod yymmdd10.;
	etl_load_dt=1637383111; valfromd='31dec2014'd; valtod='31dec7000'd; key1=1; key2=1; nokey="old";
run;
data WORK.TESTNEW; 
	length key1 key2 8 nokey $30;
	key1=1; key2=3; nokey="new";
run;
data WORK.TESTTOMODIFY; 
	length key1 key2 8 nokey $30;
	key1=1; key2=1; nokey="mod";
run;
data WORK.TESTTOCLOSE; 
	length key1 key2 8;
	key1=1; key2=1;
run;
%etl_load_sas_simple_vfvt(
		 inTargetLib=WORK
		,inTargetTblName=TESTTARGET
		,inToCloseDs=WORK.TESTTOCLOSE
		,inToModifyDs=WORK.TESTTOMODIFY
		,inNewDs=WORK.TESTNEW
		,inKeyCols=key1 key2
		,inValidDate='30JUN2015'd
		,inVFVTDateFormat=DATE
	);
Result:
Update of WORK.TESTTARGET finished
Records closed=0 , modified=1
Input datasets were overlapping:
Keys intended to be modified but never existed=0
Records intended to be closed but never existed=0
Records intended to be both closed and modified=1
Append new records to WORK.TESTTARGET finished
Records added=1

Case09: empty target dataset, type=DATETIME
proc datasets nolist nodetails lib=work; delete TESTTARGET TESTTOCLOSE TESTTOMODIFY TESTNEW; run;
data WORK.TESTTARGET; 
	length etl_load_dt valfromdt valtodt key1 key2 8 nokey $30;
	format etl_load_dt valfromdt valtodt datetime20.;
	delete; 
run;
data WORK.TESTNEW; 
	length key1 key2 8 nokey $30;
	key1=1; key2=1; nokey="old";
run;
data WORK.TESTTOMODIFY; 
	length key1 key2 8 nokey $30;
	delete; 
run;
data WORK.TESTTOCLOSE; 
	length key1 key2 8;
	delete; 
run;
%etl_load_sas_simple_vfvt(
		 inTargetLib=WORK
		,inTargetTblName=TESTTARGET
		,inToCloseDs=WORK.TESTTOCLOSE
		,inToModifyDs=WORK.TESTTOMODIFY
		,inNewDs=WORK.TESTNEW
		,inKeyCols=key1 key2
		,inValidDate='30JUN2015:0:0:0'dt
		,inVFVTDateFormat=DATETIME
	);
Result:
Update of WORK.TESTTARGET finished
Records closed=0 , modified=0
Append new records to WORK.TESTTARGET finished
Records added=1


Case10: overlapping source daatset, type=DATETIME
proc datasets nolist nodetails lib=work; delete TESTTARGET TESTTOCLOSE TESTTOMODIFY TESTNEW; run;
data WORK.TESTTARGET; 
	length etl_load_dt valfromdt valtodt key1 key2 8 nokey $30;
	format etl_load_dt valfromdt valtodt datetime20.;
	etl_load_dt=1637383111; valfromdt='31dec2014:0:0:0'dt; valtodt='31dec7000:0:0:0'dt; key1=1; key2=1; nokey="old";
run;
data WORK.TESTNEW; 
	length key1 key2 8 nokey $30;
	key1=1; key2=3; nokey="new";
run;
data WORK.TESTTOMODIFY; 
	length key1 key2 8 nokey $30;
	key1=1; key2=1; nokey="mod";
run;
data WORK.TESTTOCLOSE; 
	length key1 key2 8;
	key1=1; key2=1;
run;
%etl_load_sas_simple_vfvt(
		 inTargetLib=WORK
		,inTargetTblName=TESTTARGET
		,inToCloseDs=WORK.TESTTOCLOSE
		,inToModifyDs=WORK.TESTTOMODIFY
		,inNewDs=WORK.TESTNEW
		,inKeyCols=key1 key2
		,inValidDate='30JUN2015:0:0:0'dt
		,inVFVTDateFormat=DATETIME
	);
Result:
NO-KEY columns identified: NOKEY
Update of WORK.TESTTARGET finished
Records closed=0 , modified=1
Input datasets were overlapping:
Keys intended to be modified but never existed=0
Records intended to be closed but never existed=0
Records intended to be both closed and modified=1
Append new records to WORK.TESTTARGET finished
Records added=1


Case11: NO-KEY columns omitted, type=DATETIME
proc datasets nolist nodetails lib=work; delete TESTTARGET TESTTOCLOSE TESTTOMODIFY TESTNEW; run;
data WORK.TESTTARGET; 
	length etl_load_dt valfromdt valtodt key1 key2 8;
	format etl_load_dt valfromdt valtodt datetime20.;
	etl_load_dt=1637383111; valfromdt='31dec2014:0:0:0'dt; valtodt='31dec7000:0:0:0'dt; key1=1; key2=1;
run;
data WORK.TESTNEW; 
	length key1 key2 8;
	key1=1; key2=3;
run;
data WORK.TESTTOMODIFY; 
	length key1 key2 8;
	key1=1; key2=1;
run;
data WORK.TESTTOCLOSE; 
	length key1 key2 8;
	key1=1; key2=1;
run;
%etl_load_sas_simple_vfvt(
		 inTargetLib=WORK
		,inTargetTblName=TESTTARGET
		,inToCloseDs=WORK.TESTTOCLOSE
		,inToModifyDs=WORK.TESTTOMODIFY
		,inNewDs=WORK.TESTNEW
		,inKeyCols=key1 key2
		,inValidDate='30JUN2015:0:0:0'dt
		,inVFVTDateFormat=DATETIME
	);
Eredmény:
NO-KEY columns not found
Update of WORK.TESTTARGET finished
Records closed=0 , modified=1
Input datasets were overlapping:
Keys intended to be modified but never existed=0
Records intended to be closed but never existed=0
Records intended to be both closed and modified=1
Append new records to WORK.TESTTARGET finished
Records added=1

Case12: try to modify and close non-existing keys, type=DATETIME
proc datasets nolist nodetails lib=work; delete TESTTARGET TESTTOCLOSE TESTTOMODIFY TESTNEW; run;
data WORK.TESTTARGET; 
	length etl_load_dt valfromdt valtodt key1 key2 8 nokey $30;
	format etl_load_dt valfromdt valtodt datetime20.;
	etl_load_dt=1637383111; valfromdt='31dec2014:0:0:0'dt; valtodt='31dec7000:0:0:0'dt; key1=1; key2=1; nokey="old";
run;
data WORK.TESTNEW; 
	length key1 key2 8 nokey $30;
	key1=1; key2=3; nokey="new";
run;
data WORK.TESTTOMODIFY; 
	length key1 key2 8 nokey $30;
	key1=2; key2=1; nokey="mod";
run;
data WORK.TESTTOCLOSE; 
	length key1 key2 8;
	key1=3; key2=1;
run;
%etl_load_sas_simple_vfvt(
		 inTargetLib=WORK
		,inTargetTblName=TESTTARGET
		,inToCloseDs=WORK.TESTTOCLOSE
		,inToModifyDs=WORK.TESTTOMODIFY
		,inNewDs=WORK.TESTNEW
		,inKeyCols=key1 key2
		,inLoadDt='30JUN2015:0:0:0'dt
		,inVFVTDateFormat=DATETIME
	);
Result:
NO-KEY columns identified: NOKEY
Update of WORK.TESTTARGET finished
Records closed=0 , modified=0
Input datasets were overlapping:
Keys intended to be modified but never existed=1
Records intended to be closed but never existed=1
Records intended to be both closed and modified=0
Append new records to WORK.TESTTARGET finished
Records added=1

Case13: some source datasets left out, type=DATETIME
proc datasets nolist nodetails lib=work; delete TESTTARGET TESTTOCLOSE TESTTOMODIFY TESTNEW; run;
data WORK.TESTTARGET; 
	length etl_load_dt valfromdt valtodt key1 key2 8 nokey $30;
	format etl_load_dt valfromdt valtodt datetime20.;
	etl_load_dt=1637383111; valfromdt='31dec2014:0:0:0'dt; valtodt='31dec7000:0:0:0'dt; key1=1; key2=1; nokey="old";
run;
data WORK.TESTNEW; 
	length key1 key2 8 nokey $30;
	key1=1; key2=3; nokey="new";
run;
%etl_load_sas_simple_vfvt(
		 inTargetLib=WORK
		,inTargetTblName=TESTTARGET
		,inToCloseDs=
		,inToModifyDs=
		,inNewDs=WORK.TESTNEW
		,inKeyCols=key1 key2
		,inValidDate='30JUN2015:0:0:0'dt
		,inVFVTDateFormat=DATETIME
	);
Result:
Param inToCloseDs left empty so an empty table WORK.QWERTZQWERTZINTOCLOSEDS has been created with only the KEY cols.
Param inToModifyDs left empty so an empty table WORK.QWERTZQWERTZINTOMODIFYDS has been created with only the KEY cols.
NO-KEY columns not found
Update of WORK.TESTTARGET finished
Records closed=0 , modified=0
Append new records to WORK.TESTTARGET finished
Records added=1

Case14: inValidDate left empty, type=DATE
proc datasets nolist nodetails lib=work; delete TESTTARGET TESTTOCLOSE TESTTOMODIFY TESTNEW; run;
data WORK.TESTTARGET; 
	length valfromd valtod key1 key2 8 nokey $30;
	format valfromd valtod datetime20.;
	valfromd='31dec2014'd; valtod='31dec7000'd; key1=1; key2=1; nokey="old";
run;
data WORK.TESTNEW; 
	length key1 key2 8 nokey $30;
	key1=1; key2=3; nokey="new";
run;
%etl_load_sas_simple_vfvt(
		 inTargetLib=WORK
		,inTargetTblName=TESTTARGET
		,inToCloseDs=
		,inToModifyDs=
		,inNewDs=WORK.TESTNEW
		,inKeyCols=key1 key2
		,inValidDate=
		,inVFVTDateFormat=DATE
	);
Result:
Param inToCloseDs left empty so an empty table WORK.QWERTZQWERTZINTOCLOSEDS          has been created with only the KEY cols.
Param inToModifyDs left empty so an empty table WORK.QWERTZQWERTZINTOMODIFYDS         has been created with only the KEY cols.
NO-KEY columns not found
Update of WORK.TESTTARGET finished
Records closed=0 , modified=0
Append new records to work.tmptargettable WORK.TESTTARGET finished
Records added=1
Replacement of WORK.TESTTARGET succeeded
Records added=1
MACRO etl_load_sas_simple_vfvt ENDED

Case15: inValidDate left empty, type=DATETIME
Param inToCloseDs left empty so an empty table WORK.QWERTZQWERTZINTOCLOSEDS          has been created with only the KEY cols.
Param inToModifyDs left empty so an empty table WORK.QWERTZQWERTZINTOMODIFYDS         has been created with only the KEY cols.
NO-KEY columns not found
Update of WORK.TESTTARGET finished
Records closed=0 , modified=0
Append new records to work.tmptargettable WORK.TESTTARGET finished
Records added=1
Replacement of WORK.TESTTARGET succeeded
Records added=1
MACRO etl_load_sas_simple_vfvt ENDED
Result:

Case16: complex case
%let etl_upd_d=%sysfunc(date());

data basetable;
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

data toclose;
	length key $3
		   value 8
		   ;

	format valfromd valtod yymmdd10.;

	key = "K1"; output;
run;

data tomod;
	length key $3
		   value 8
		   ;

	format valfromd valtod yymmdd10.;

	key = "K3"; value=50; output;
run;

data reopen;
	length key $3
		   value 8
		   ;

	format valfromd valtod yymmdd10.;

	key = "K2"; value=500; output;
run;

data basetable_befupdate;
	set basetable;
run;

%etl_load_sas_simple_vfvt(
		 inTargetLib=WORK
		,inTargetTblName=basetable
		,inToCloseDs=WORK.toclose
		,inToModifyDs=WORK.tomod
		,inNewDs=WORK.reopen
		,inKeyCols=key
		,inValidDate=&etl_upd_d.
		,inVFVTDateFormat=DATE
	);
*/
