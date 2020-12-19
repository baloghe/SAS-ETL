*/------------------------------------------------------------------------*
| Usage: SAS Hash object wrapper                                          |
| 		                                                                  |
| Parameters:                                                             |
| 	inTbl  			MANDATORY 	table to be joined in (must fit in memory)|
| 	inKey  			MANDATORY 	KEY columns                               |
| 	inCol 			MANDATORY 	DATA columns (to be joined in)            |
|                                                                         |
| Example:                                                                |
| 	%shrink_table(WORK.CONNIDS_DUPS);                                     |
|                                                                         |
| Checks:                                                                 |
| 	table (to be shrinked) exists                                         |
*-------------------------------------------------------------------------*/
%macro hashjoin(inTbl, inKey, data);
%local	dsid
		i
		j
		l
		m
		_length
		av_nm
		av_ty
		av_le
		structure
		av_prefix
		hashname
		uppChars
		lowChars
		chars
		rc
		tmpkey
		tmpdata
		tmpnvars
		;
%let dsid=%sysfunc(open(&inTbl.,i));
%if(&dsid.=0) %then %do;
	%put ERROR: HashJoin :: source table &inTbl. does not exist! exit macro;
	%goto exitmacro;
%end;

/* Check KEY fields existence */
%let i = 1;
%do %while (%qscan(&inKey., &i.) ne);
	%let av_nm = %qscan(&inKey., &i.);
	%let tmpnvars=%sysfunc(attrn(&dsid., nvars));
	%let av_ty=;
	%do j = 1 %to %sysfunc(attrn(&dsid., nvars));
		%if %upcase(%sysfunc(varname(&dsid., &j.))) =  %upcase(&av_nm.) %then %do;
			%let av_ty = %sysfunc(vartype(&dsid.,&j.));
		%end;
	%end;
	%if &av_ty= %then %do;
		%put ERROR: HashJoin (&inTbl.) :: KEY[&i.]==&av_nm. does not exist in joined table! exit macro;
		%let rc=%sysfunc(close(&dsid));
		%goto exitmacro;
	%end;
	%let i = %eval(&i. + 1);
%end;

/* Define DATA fields */
%let i = 1;
%let structure =;
%do %while (%qscan(&inCol., &i.) ne);
	%let av_nm = %qscan(&inCol., &i.);
	%let tmpnvars=%sysfunc(attrn(&dsid., nvars));
	%let av_ty=;
	%do j = 1 %to %sysfunc(attrn(&dsid., nvars));
		%if %upcase(%sysfunc(varname(&dsid., &j.))) =  %upcase(&av_nm.) %then %do;
			%let av_ty = %sysfunc(vartype(&dsid.,&j.));
			%let av_le = %sysfunc(varlen(&dsid.,&j.));
			%if &av_ty. = C %then %let av_prefix = $; %else %let av_prefix =;			
			%let structure = &structure. &av_nm. &av_prefix.&av_le.;
		%end;
	%end;
	%if &av_ty= %then %do;
		%put ERROR: HashJoin (&inTbl.) :: DATA[&i.]==&av_nm. does not exist in joined table! exit macro;
		%let rc=%sysfunc(close(&dsid));
		%goto exitmacro;
	%end;
	%let i = %eval(&i. + 1);
%end;
%let rc=%sysfunc(close(&dsid));

length &structure.;

%let hashname =;
%let uppChars = ABCDEFGHIJKLMNOPQRSTUVWXYZ;
%let lowChars = abcdefghijklmnopqrstuvwxyz;
%let chars = &uppChars&lowChars;
%let _length = %length(&chars) + 1;
%do k=1 %to 15;
	%let hashname =  &hashname%cmpres(%substr(&chars,%sysfunc(floor(%sysfunc(ranuni(0))*(&_length-1)+1)),1));
%end;

if _n_=1 then do; 

declare hash &hashname.(dataset: "&inTbl."); 

%let l = 1;
%let tmpkey=%qscan(&inKey., &l.);
%do %while (&tmpkey. ne);
	_rc = &hashname..defineKey("&tmpkey.");
	%let l = %eval(&l. + 1);
	%let tmpkey=%qscan(&inKey., &l.);
%end;

%let m = 1;
%let tmpdata=%qscan(&inCol., &m.);
%do %while (&tmpdata. ne);
	_rc = &hashname..defineData("&tmpdata.");
	%let m = %eval(&m. + 1);
	%let tmpdata=%qscan(&inCol., &m.);
%end;

_rc = &hashname..defineDone(); 
end; 
_rc=&hashname..find();
DROP _rc;

%exitmacro:
%mend hashjoin;
/* Case01: everything OK
data a1;
do col1 = 1 to 10;
	do col2 = 11 to 20;
	output;
	end;
end;
run;

data a2;
do col1 = 5 to 10;
	do col2 = 11 to 15;
		do col3 = 20 to 22;
			do col4 = 30 to 33;
				col3col4 = col3 * col4;
				col2_col3 = col2 - col3;
				output;
			end;
		end;
	end;
end;
run;
options mprint mlogic symbolgen;
data b1;
 set a1;
 %hashjoin(a2, col1 col2, col3 col4);
run;
*/
/* Case02: table does not exist
data a1;
do col1 = 1 to 10;
	do col2 = 11 to 20;
	output;
	end;
end;
run;

data a2;
do col1 = 5 to 10;
	do col2 = 11 to 15;
		do col3 = 20 to 22;
			do col4 = 30 to 33;
			output;
			end;
		end;
	end;
end;
run;
options mprint mlogic symbolgen;
data b1;
 set a1;
 %hashjoin(nosuchtable, col1 col2, col3 col4);
run;
*/
/* Case03: table exists but KEY field does not exists in lookup table
data a1;
do col1 = 1 to 10;
	do col2 = 11 to 20;
		output;
	end;
end;
run;

data a2;
do col1 = 5 to 10;
	do col20 = 11 to 15;
		do col3 = 20 to 22;
			do col4 = 30 to 33;
				output;
			end;
		end;
	end;
end;
run;
options mprint mlogic symbolgen;
data b1;
 set a1;
 %hashjoin(a2, col1 col2, col3 col4);
run;
*/
/* Case04: table exists but DATA field does not exists in lookup table
data a1;
do col1 = 1 to 10;
	do col2 = 11 to 20;
		output;
	end;
end;
run;

data a2;
do col1 = 5 to 10;
	do col2 = 11 to 15;
		do col30 = 20 to 22;
			do col4 = 30 to 33;
				output;
			end;
		end;
	end;
end;
run;
options mprint mlogic symbolgen;
data b1;
 set a1;
 %hashjoin(a2, col1 col2, col3 col4);
run;
*/
/* Case05: table exists but KEY fields type is mismatched
data a1;
length col1 8 col2 $5;
do col1 = 1 to 10;
	do col2num = 11 to 20;
		col2=compress(put(col2num,best10.));
		output;
	end;
end;
run;

data a2;
do col1 = 5 to 10;
	do col2 = 11 to 15;
		do col3 = 20 to 22;
			do col4 = 30 to 33;
			output;
			end;
		end;
	end;
end;
run;
options mprint mlogic symbolgen;
data b1;
 set a1;
 %hashjoin(a2, col1 col2, col3 col4);
run;
*/