# SAS-ETL
SAS helper macros for peforming ETL tasks

<p><b>%etl_load_sas_simple_vfvt</b></p>
<p>would load the target table provided that the New/To-Be-Modified/To-Be-Closed records have already been separated in different temp tables</p>
<p><b>%etl_simple_vfvt_wrapper</p></b>
<p>would provide an easy-to-understand „interface” to the user, in which only the target and source tables, natural key and other attributes list and some other parameters should be provided</p>
<p><b>%etl_test_connection</p></b>
<p>would check the existence of one or more given datasets</p>
<p><b>%hashjoin</p></b>
<p>simple wrapper for SAS Hash object</p>
<p><b>%shrink_table</p></b>
<p>shrinks char columns to max(length(.))</p>
<p><b>%etl_table_dupker</p></b>
<p>checks for key uniqueness</p>
<p><b>%ruleBasedAssignment</p></b>
<p>enhanced maptable lookup: <ul>
<li>joker char (e.g. #) acception</li>
<li>rule ordering in case of multiple match</li>
<li>allowing for the following operators: IN, NOTIN, LIKE, NOTLIKE, EQ, NE, LE, LT, GE, GT + LIKE, NOTLIKE: through PRXMATCH</li>
</p>