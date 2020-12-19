*/------------------------------------------------------------------------*
| Assigns a value from a parameter table to all records in the given data |
| table by applying the rules given in the macro call                     |
| Inputs:                                                                 |
|    inRuleTable	Mandatory	rule table                                |
|    inDataTable    Mandatory	data table to be read in                  |
|    outDataTable	Mandatory   output table                              |
|    outAssignedCol	Mandatory   column (in rule table) to be assigned to  |
|                               each record in data table                 |
|    inRuleDef		Mandatory   rules to be applied in fashion            |
|									DataColumnNm:Operator:RuleColumnNm    |
|                                      triples                            |
|                               OR DataColumnNm Operator RuleColumnNm     |
|                                      triples                            |
|								    spearated by spaces/colons            |
|								e.g.                                      |
|                                 DCol1:NOTIN:RCol1 DCol2:LIKE:RCol2      |
|                               where DCol1, DCol2 reside in data table,  |
|                                     RCol1, RCol2 reside in rule table,  |
|                                 and outAssignedCol should be assigned   |
|                                     to a record when                    |
|                                         DCol1 not in (RCol1)            |
|                                     AND DCol2 like RCol2      holds     |
|								eligible operators:                       |
|								IN,NOTIN,LIKE,NOTLIKE,EQ,NE,LE,LT,GE,GT   |
|								LIKE, NOTLIKE: applied through PRXMATCH   |
|    inOrderCol		Mandatory   ordering column in rule table to choose a |
|								single rule when more than one would fit  |
|    inJokerChar	Optional	Joker character for IN, EQ                |
|								default value: #                          |
|                                                                         |
*-------------------------------------------------------------------------*/
%macro ruleBasedAssignment(
			 inRuleTable=
			,inDataTable=
			,outDataTable=
			,outAssignedCol=
			,inRuleDef=
			,inOrderCol=
			,inJokerChar=#
		);

%local	tmpchkinparams
		tmpRuleColumnNms
		tmpRuleColumnCs
		tmpDataColumnNms
		tmpDataColumnNs
		tmpEligibleOperators
		tmpOperatorReqElemCnt
		dsid
		rc
		i
		tmp
		tmptp
		recnum
		tmprulecnt
		tmpRLs
		tmpOPs
		tmpDTs
		outAssignedColType
		outAssignedColLen
		inOrderColType
		tmpAssColInDataTbl
		;

/* Eligible (already implemented) operators with their respqctive element number */
%let tmpEligibleOperators =IN NOTIN LIKE NOTLIKE EQ NE LE LT GE GT;
%let tmpOperatorReqElemCnt=3 3 3 3 3 3 3 3 3 3;

/* Check incoming params  -- START */
%let tmpchkinparams=0;
%let tmpRuleColumnNms=;
%let tmpRuleColumnCs=;
%let tmpDataColumnNms=;
%let tmpDataColumnNs=;
%if "&outAssignedCol" eq "" %then %do;
	%put ERROR: PARAM outAssignedCol left empty!;
	%let tmpchkinparams=1;
%end;
%if "&inOrderCol" eq "" %then %do;
	%put ERROR: PARAM inOrderCol left empty!;
	%let tmpchkinparams=1;
%end;
%if "&inRuleTable" eq "" %then %do;
	%put ERROR: PARAM inRuleTable left empty!;
	%let tmpchkinparams=1;
%end; %else %do;
	%if not(%sysfunc(exist(&inRuleTable))) %then %do;
		%put ERROR: Table &inRuleTable does not exist!;
		%let tmpchkinparams=1;
	%end; %else %do;
		/* obtain variable list into -> tmpRuleColumnNms */
		%let dsid=%sysfunc(open(&inRuleTable));
		%if &dsid eq 0 %then %do;
			%put ERROR: table &inRuleTable. could not be opened!;
		%end; %else %do;
			%let outAssignedColType=;
			%let inOrderColType=;
			%let rc=%sysfunc(fetch(&dsid));
			%do i=1 %to %sysfunc(attrn(&dsid,nvars));
				%let tmpRuleColumnNms = &tmpRuleColumnNms. %upcase(%sysfunc(VARNAME(&dsid,&i)));
				/* preserve variable list of CHAR vars  */
				%if %sysfunc(VARTYPE(&dsid,&i))=C %then %do;
					%let tmpRuleColumnCs = &tmpRuleColumnCs. %upcase(%sysfunc(VARNAME(&dsid,&i)));
				%end;
				/* check if outAssignedCol exists in inRuleTable */
				%if "&outAssignedCol" ne "" 
				    AND %upcase(%sysfunc(VARNAME(&dsid,&i))) eq %upcase(&outAssignedCol) %then %do;
					/* if YES then save its type */
					%let outAssignedColType=%sysfunc(VARTYPE(&dsid,&i));
					%let outAssignedColLen=%sysfunc(VARLEN(&dsid,&i));
				%end;
				/* check if inOrderCol exists in inRuleTable */
				%if "&inOrderCol" ne "" 
				    AND %upcase(%sysfunc(VARNAME(&dsid,&i))) eq %upcase(&inOrderCol) %then %do;
					/* if YES then save its type */
					%let inOrderColType=%sysfunc(VARTYPE(&dsid,&i));
				%end;
			%end;
			/* outAssignedColType empty => outAssignedCol does not exist in inRuleTable */
			%if "&outAssignedColType" eq "" %then %do;
				%put ERROR: Table &inRuleTable does not contain column &outAssignedCol!;
				%let tmpchkinparams=1;
			%end;
			/* inOrderColType empty => inOrderColType does not exist in inRuleTable */
			%if "&inOrderColType" eq "" %then %do;
				%put ERROR: Table &inRuleTable does not contain column &inOrderCol!;
				%let tmpchkinparams=1;
			%end; %else %if "&inOrderColType" ne "N" %then %do;
				%put ERROR: Column &inOrderCol in table &inRuleTable is of type &inOrderColType instead of being NUMERIC!;
				%let tmpchkinparams=1;
			%end;
		%end;
		%let rc=%sysfunc(close(&dsid));
	%end;
%end;
%if "&inDataTable" eq "" %then %do;
	%put ERROR: PARAM inDataTable left empty!;
	%let tmpchkinparams=1;
%end; %else %do;
	%if not(%sysfunc(exist(&inDataTable))) %then %do;
		%put ERROR: Tabe &inDataTable does not exist!;
		%let tmpchkinparams=1;
	%end; %else %do;
		/* obtain variable list into -> tmpDataColumnNms */
		%let dsid=%sysfunc(open(&inDataTable));
		%if &dsid eq 0 %then %do;
			%put ERROR: table &inDataTable. could not be opened!;
		%end; %else %do;
			%let rc=%sysfunc(fetch(&dsid));
			%let tmpAssColInDataTbl=N;
			%do i=1 %to %sysfunc(attrn(&dsid,nvars));
				%let tmpDataColumnNms = &tmpDataColumnNms. %upcase(%sysfunc(VARNAME(&dsid,&i)));
				/* subset numeric fields into a separate list -> tmpDataColumnNs */
				%if %sysfunc(VARTYPE(&dsid,&i))=N %then %do;
					%let tmpDataColumnNs = &tmpDataColumnNs. %upcase(%sysfunc(VARNAME(&dsid,&i)));
				%end;
				/* check if outAssignedCol is present in inDataTable */
				%if %upcase(&outAssignedCol.)=%upcase(%sysfunc(VARNAME(&dsid,&i))) %then %do;
					%let tmpAssColInDataTbl=Y;
					%put NOTE: Table &inDataTable already contains a column &outAssignedCol. The column will be overwritten!;
				%end;
			%end;
		%end;
		%let rc=%sysfunc(close(&dsid));
	%end;
%end;
%if "&outDataTable" eq "" %then %do;
	%put ERROR: PARAM outDataTable left empty!;
	%let tmpchkinparams=1;
%end; %else %do;
	%if %sysfunc(exist(&outDataTable)) %then %do;
		%put NOTE: &outDataTable already exists, will be overwritten by this macro.;
	%end;
%end;
%if "&inRuleDef" eq "" %then %do;
	%put ERROR: PARAM inRuleDef left empty!;
	%let tmpchkinparams=1;
%end; %else %do;
	/* break down incoming list into triplets of {DataColumn, Operator, RuleColumn}
	   and check validity against variable / operator list
	*/
	%let i=1;
	%let tmp=%scan(&inRuleDef.,&i.,' :');

	%let tmprulecnt=0;
	%let tmpDTs=;	
	%let tmpOPs=;
	%let tmpRLs=;
	%do %while( "&tmp" ^= "");
		/* modulo 3 = 1->DataCol, 2->Operator, 0->RuleCol */
		%let tmptp=%sysfunc(mod(&i.,3));

		%if &tmptp=1 %then %do;
			/* check against Data varlist */
			%if %index(&tmpDataColumnNms, %upcase(&tmp) ) le 0 %then %do;
				%put ERROR: &i th element of RuleDef=&tmp is not a column in &inDataTable!;
				%let tmpchkinparams=1;
			%end;
			/* build ith rule / Data component */
			%let tmpDTs = &tmpDTs. %upcase(&tmp);
		%end;
		%else %if &tmptp=2 %then %do;
			/* check against Operator list */
			%if %index(&tmpEligibleOperators, %upcase(&tmp) ) le 0 %then %do;
				%put ERROR: &i th element of RuleDef=&tmp is not an eligible operator!;
				%let tmpchkinparams=1;
			%end;
			/* build ith rule / Operator component */
			%let tmpOPs = &tmpOPs. %upcase(&tmp);
		%end;
		%else %do;
			/* check against Rule varlist */
			%if %index(&tmpRuleColumnNms, %upcase(&tmp) ) le 0 %then %do;
				%put ERROR: &i th element of inRuleDef=&tmp is not a column in &inRuleTable!;
				%let tmpchkinparams=1;
			%end;
			/* build ith rule / Operator component */
			%let tmpRLs = &tmpRLs. %upcase(&tmp);
			/* 3rd rule element => increase rule counter */
			%let tmprulecnt=%eval(&tmprulecnt + 1);
		%end;

		/* step counter */
		%let i=%eval(&i + 1);
		%let tmp=%scan(&inRuleDef.,&i.,' :');
	%end;
	/* check if triplets are really triplets */
	%if &tmptp ne 0 %then %do;
		%put ERROR: Number of elements in inRuleDef is not a multiple of 3!;
		%let tmpchkinparams=1;		
	%end;
%end;

/* Problem => exit */
%if &tmpchkinparams ne 0 %then %do;
	%put ruleBasedAssignment :: Param check failed! exit macro;
	%put proper usage: ruleBasedAssignment(inRuleTable=,inDataTable=,outDataTable=,outAssignedCol=,inRuleDef=,inOrderCol=);
	%goto exitmacro;
%end;
%put Number of rules=&tmprulecnt.;
/*
%put tmpRuleColumnCs=&tmpRuleColumnCs.;
%put tmpDataColumnNs=&tmpDataColumnNs.;
*/
/* Check incoming params  -- END */

/* Construct rules  -- START */
/* Sort by inOrderCol */
proc sort data=&inRuleTable.;
	by &inOrderCol.;
run;
/* Resulting rules enclosed in a Catalog file */
filename    _rbl_gen catalog "work.rbl.rbl.source";

%let recnum=0;
data _null_ /*valami*/;
    set     &inRuleTable. end=_e;
    file    _rbl_gen;

	length recnum 8;
	retain recnum 0;

	if _N_=1 then do;
        put     "SELECT;";
		recnum = recnum + 1;
	end;

	/* for each observation: determine if the nth rule IS joker or not */
	%do i=1 %to &tmprulecnt.;
		%if %index(&tmpRuleColumnCs, %scan(&tmpRLs.,&i.,' ') ) gt 0
			and (   "%scan(&tmpOPs.,&i.,' ')" eq "LIKE"
			     or "%scan(&tmpOPs.,&i.,' ')" eq "IN"
			     or "%scan(&tmpOPs.,&i.,' ')" eq "NOTIN"
			     or "%scan(&tmpOPs.,&i.,' ')" eq "EQ"
			     or "%scan(&tmpOPs.,&i.,' ')" eq "NE"
			     or "%scan(&tmpOPs.,&i.,' ')" eq "GT"
			     or "%scan(&tmpOPs.,&i.,' ')" eq "GE"
			     or "%scan(&tmpOPs.,&i.,' ')" eq "LT"
			     or "%scan(&tmpOPs.,&i.,' ')" eq "LE"
			    )
		%then %do;
			length	__________________&i 3
					;
			__________________&i = 0;
			if( %scan(&tmpRLs.,&i.,' ') eq "&inJokerChar." ) then do;
				__________________&i = 1;
			end;
		%end;
	%end;/* next rule */

	/* special handling... */
	length filterClause $10000;

	/* start WHEN -> 1 stays for "always TRUE" */
	filterClause = "when ( 1";
	/* translate rules 
		eg. "ENTITY_CD IN ('EBH' 'ELL')"
			where 	ENTITY_CD should come from tmpDTs
					IN should come from tmpOPs
					( and ) should be inserted if the operator requires it
					'EBH' 'ELL' should be read from the column selected by tmpRLs
	*/
	%do i=1 %to &tmprulecnt.;

		/* ith rule starts with AND + opening bracket */
		filterClause = strip(filterClause) || ' and '
						;

		/* handle Joker */
		if( __________________&i eq 1 ) then do;
			filterClause = strip(filterClause) || " '&inJokerChar.' eq '&inJokerChar.' " 
						;
		end; 
		else do; /* non-joker case */

			/* LIKE and NOT LIKE has to be inserted into PRXMATCH... */
			%if %scan(&tmpOPs.,&i.,' ') eq LIKE %then %do;
				filterClause = strip(filterClause)
							|| "prxmatch('" || strip(%scan(&tmpRLs.,&i.,' ')) || "', %scan(&tmpDTs.,&i.,' ') ) gt 0 "
							;
			%end; %else %if %scan(&tmpOPs.,&i.,' ') eq NOTLIKE %then %do;
				filterClause = strip(filterClause)
							|| "prxmatch('" || strip(%scan(&tmpRLs.,&i.,' ')) || "', %scan(&tmpDTs.,&i.,' ') ) le 0 "
							;
			%end; %else %do;
				/* anything else */
				filterClause = strip(filterClause) || ' '
							|| "%scan(&tmpDTs.,&i.,' ') "
							|| %if %scan(&tmpOPs.,&i.,' ') eq NOTIN %then %do; 'NOT IN ' %end;
							   %else %do; "%scan(&tmpOPs.,&i.,' ')" || " " %end;

							||       %if    "%scan(&tmpOPs.,&i.,' ')"="IN"
							             or "%scan(&tmpOPs.,&i.,' ')"="NOTIN" %then %do; '(' || strip(%scan(&tmpRLs.,&i.,' ')) || ') ' %end;
							   %else %do;
							   		/* beware: in case of {EQ, NE, LT, GT, LE, GE} the Data should be compared to a compatible type! */
							   		%if %index(&tmpDataColumnNs, %scan(&tmpDTs.,&i.,' ') )
										and (   "%scan(&tmpOPs.,&i.,' ')" eq "EQ"
										     or "%scan(&tmpOPs.,&i.,' ')" eq "NE"
										     or "%scan(&tmpOPs.,&i.,' ')" eq "GT"
										     or "%scan(&tmpOPs.,&i.,' ')" eq "GE"
										     or "%scan(&tmpOPs.,&i.,' ')" eq "LT"
										     or "%scan(&tmpOPs.,&i.,' ')" eq "LE"
											) %then %do;
										strip(%scan(&tmpRLs.,&i.,' '))
							   		%end; %else %do;
										"'" || strip(%scan(&tmpRLs.,&i.,' ')) || "' " 
									%end;
							   %end;
							;
			%end;
		end;/* end of non-Joker case */
	%end;/* next operation in the same rule */
	/* close WHEN with a bracket */
	filterClause = strip(filterClause) || ") do;"
									   || " &inOrderCol=" || strip(put(&inOrderCol.,best32.)) || "; "
										;
	filterClause = strip(filterClause) || " &outAssignedCol="
					                   || %if &outAssignedColType=C %then %do; "'" %end; %else %do; "" %end;
									   || %if &outAssignedColType=N %then %do; strip(put(&outAssignedCol,best32.)) %end;
										                            %else %do; strip(&outAssignedCol) %end;
					                   || %if &outAssignedColType=C %then %do; "'" %end; %else %do; "" %end;
									   || ";"
									   ;
	filterClause = strip(filterClause) || " end;";

	/* write out single rule */
	filterClause = strip(filterClause);
	put filterClause;
	recnum = recnum + 1;

	/* close SELECT */
    if _e then do;
    	put "otherwise;";
    	put "end;";
		recnum = recnum + 2;
		call symput ("recnum",strip(put(recnum,best32.-l)));
    end;
run;
%put CATALOG RBL recnum :: recnum=&recnum;

/* Construct rules  -- END */

/* Apply rules  -- START */
data &outDataTable;
	set &inDataTable %if &tmpAssColInDataTbl=Y %then %do; (drop=&outAssignedCol) %end; ;
	/* invoke rule-based lookup */
	length	&outAssignedCol %if &outAssignedColType=C %then %do; $ %end; &outAssignedColLen
			&inOrderCol	8
			;
	%include _rbl_gen;
run;
/* Apply rules  -- END */

%exitmacro:
%mend ruleBasedAssignment;