*/------------------------------------------------------------------------*
| Usage: identifies duplicate rows (according to a given set of 		  |
|            KEY columns) in a table                                      |
| 		                                                                  |
| Parameters:                                                             |
| 	inTbl  			MANDATORY 	table to be shrinked                      |
|                                                                         |
| Example:                                                                |
| 	%shrink_table(WORK.CONNIDS_DUPS);                                     |
|                                                                         |
| Checks:                                                                 |
| 	table (to be shrinked) exists                                         |
--------------------------------------------------------------------------*/
%macro shrink_table(
			inTbl
		);

%local	dsid
		rc
		i j
		varlist
		varlistC
		varlist_len
		varlistC_len
		varlens
		varlensC
		varfmts
		tmpvarname
		tmptableempty
		;

%let dsid=%sysfunc(open(&inTbl));
%if &dsid eq 0 %then %do;
	%put shrink_table :: table &inTbl. could not be opened! exit macro;
	%goto exitmacro;
%end; %else %do;
	%let rc=%sysfunc(fetch(&dsid));
	%let varlist=;
	%let varlist_len=0;
	%let varlistC=;
	%let varlistC_len=0;
	%let varfmts=;
	%let varlens=;
	%do i=1 %to %sysfunc(attrn(&dsid,nvars));
		/* all vars */
		%let varlist = &varlist. %upcase(%sysfunc(VARNAME(&dsid,&i)));
		%let varlens = &varlens. %sysfunc(VARLEN(&dsid,&i));
		%let varlist_len=%eval(&varlist_len.+1);
		/* second list for char vars only */
		%if %sysfunc(VARTYPE(&dsid,&i)) eq %str(C) %then %do;
			%let varlistC = &varlistC. %upcase(%sysfunc(VARNAME(&dsid,&i)));
			%let varlistC_len=%eval(&varlistC_len.+1);
		%end;
		%if "%sysfunc(VARFMT(&dsid,&i))" ^= "" %then %do;
			%let varfmts = &varfmts %upcase(%sysfunc(VARNAME(&dsid,&i))) %sysfunc(VARFMT(&dsid,&i));
		%end;
	%end;/*next var*/
%end;
%put varlistC=&varlistC.;

%if "&varlistC" eq "" %then %do;
	%put varlistC is empty => nothing to shrink. Exit macro;
	%goto exitmacro;
%end;

%let tmptableempty=YES;
data _null_;
	set &inTbl(obs=1);
	call symput ("tmptableempty","NO");
run;
%if "&tmptableempty"="YES" %then %do;
	%put Table &inTbl is empty => nothing to shrink. Exit macro;
	%goto exitmacro;
%end;

/* measure char var effective lengths after strip(.) */
data _null_;
	set &inTbl.(
			keep=&varlistC.
		)
		end=vege
		;
	/* var length counters for char vars */
	length
		%do i=1 %to &varlistC_len.;
			___varlen_&i.
		%end;
		8
		;

	retain
		%do i=1 %to &varlistC_len.;
			___varlen_&i. 0
		%end;
		;
	%do i=1 %to &varlistC_len.;
		if length(strip(%scan(&varlistC.,&i.,' '))) gt ___varlen_&i. then ___varlen_&i.=length(strip(%scan(&varlistC.,&i.,' ')));
	%end;

	if(vege) then do;
		length varlensC $32000;
		varlensC= strip(varlensC)
			%do i=1 %to &varlistC_len.;
				|| " " || strip(put(___varlen_&i.,best32.))
			%end;
			;
		call symput("varlensC",strip(varlensC));
	end;
run;
%put varlensC=&varlensC.;
%let rc=%sysfunc(close(&dsid));



/* effective shrinking */
%let j=1;/*pointer for jth char var*/

%put --------> expected outcome:;
%do i=1 %to &varlist_len.;
				%let tmpvarname=%scan(&varlist.,&i.,' ');
				%if &tmpvarname eq %scan(&varlistC.,&j.,' ') %then %do;
					%put %scan(&varlistC.,&j.,' ') $ %scan(&varlensC.,&j.,' ');
					%let j=%eval(&j + 1);
				%end; %else %do;
					%put %scan(&varlist.,&i.,' ') %scan(&varlens.,&i.,' ');
				%end;
%end;

%let j=1;/*pointer for jth char var*/
data &inTbl.(
			drop=%do i=1 %to &varlistC_len.;
					______&i
			     %end;
		);

	length	%do i=1 %to &varlist_len.;
				%let tmpvarname=%scan(&varlist.,&i.,' ');
				%if &tmpvarname eq %scan(&varlistC.,&j.,' ') %then %do;
					%scan(&varlistC.,&j.,' ') $ %scan(&varlensC.,&j.,' ')
					%let j=%eval(&j + 1);
				%end; %else %do;
					%scan(&varlist.,&i.,' ') %scan(&varlens.,&i.,' ')
				%end;
			%end;
			;
	format &varfmts.;

	set &inTbl.(
			rename=(
					%do i=1 %to &varlistC_len.;
						%scan(&varlistC.,&i.,' ') = ______&i
					%end;
					)
		);

	%do i=1 %to &varlistC_len.;
		%scan(&varlistC.,&i.,' ') = strip( ______&i );
	%end;
run;

%exitmacro:
%let rc=%sysfunc(close(&dsid));
%mend shrink_table;


/*
data abc;
	length ertek 8 datum 4 szoveg $40 ertek2 8 dstmp 8;
	format ertek commax22. datum yymmdd10. dstmp datetime19.;
	datum = '30SEP2018'd;
	dstmp = datetime();
	ertek2 = date();

	ertek = 30; szoveg = "   hhh"; output;
	ertek = 30000; szoveg = "abc             "; output;
	ertek = 30000010; szoveg = "   hhh             dfg            "; output;
run;

proc contents data=abc; run;
proc sql;
	select * from abc;
quit;

%shrink_table(abc);

proc contents data=abc; run;
proc sql;
	select * from abc;
quit;
*/