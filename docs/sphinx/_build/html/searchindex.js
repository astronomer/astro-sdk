Search.setIndex({docnames:["astro","astro.dataframe","astro.ml","astro.sql","astro.sql.operators","astro.utils","index","modules"],envversion:{"sphinx.domains.c":2,"sphinx.domains.changeset":1,"sphinx.domains.citation":1,"sphinx.domains.cpp":4,"sphinx.domains.index":1,"sphinx.domains.javascript":2,"sphinx.domains.math":2,"sphinx.domains.python":3,"sphinx.domains.rst":2,"sphinx.domains.std":2,sphinx:56},filenames:["astro.rst","astro.dataframe.rst","astro.ml.rst","astro.sql.rst","astro.sql.operators.rst","astro.utils.rst","index.rst","modules.rst"],objects:{"":[[6,0,0,"-","astro"]],"astro.dataframe":[[1,1,1,"","dataframe"]],"astro.ml":[[2,1,1,"","predict"],[2,1,1,"","train"]],"astro.sql":[[3,1,1,"","append"],[3,1,1,"","merge"],[4,0,0,"-","operators"],[3,1,1,"","run_raw_sql"],[3,0,0,"-","table"],[3,1,1,"","transform"],[3,1,1,"","transform_file"],[3,1,1,"","truncate"]],"astro.sql.operators":[[4,0,0,"-","agnostic_aggregate_check"],[4,0,0,"-","agnostic_boolean_check"],[4,0,0,"-","agnostic_load_file"],[4,0,0,"-","agnostic_save_file"],[4,0,0,"-","agnostic_sql_append"],[4,0,0,"-","agnostic_sql_merge"],[4,0,0,"-","agnostic_sql_truncate"],[4,0,0,"-","agnostic_stats_check"],[4,0,0,"-","sql_dataframe"],[4,0,0,"-","sql_decorator"]],"astro.sql.operators.agnostic_aggregate_check":[[4,2,1,"","AgnosticAggregateCheck"],[4,1,1,"","aggregate_check"]],"astro.sql.operators.agnostic_aggregate_check.AgnosticAggregateCheck":[[4,3,1,"","execute"],[4,4,1,"","template_fields"]],"astro.sql.operators.agnostic_boolean_check":[[4,2,1,"","AgnosticBooleanCheck"],[4,2,1,"","Check"],[4,1,1,"","boolean_check"]],"astro.sql.operators.agnostic_boolean_check.AgnosticBooleanCheck":[[4,3,1,"","execute"],[4,3,1,"","get_expression"],[4,3,1,"","get_failed_checks"],[4,3,1,"","prep_boolean_checks_query"],[4,3,1,"","prep_results"],[4,4,1,"","template_fields"]],"astro.sql.operators.agnostic_boolean_check.Check":[[4,3,1,"","get_expression"],[4,3,1,"","get_result"]],"astro.sql.operators.agnostic_load_file":[[4,2,1,"","AgnosticLoadFile"],[4,1,1,"","load_file"]],"astro.sql.operators.agnostic_load_file.AgnosticLoadFile":[[4,3,1,"","execute"],[4,4,1,"","template_fields"],[4,3,1,"","validate_path"]],"astro.sql.operators.agnostic_save_file":[[4,2,1,"","SaveFile"],[4,1,1,"","save_file"]],"astro.sql.operators.agnostic_save_file.SaveFile":[[4,3,1,"","agnostic_write_file"],[4,3,1,"","create_table_name"],[4,3,1,"","execute"],[4,3,1,"","file_exists"],[4,4,1,"","template_fields"]],"astro.sql.operators.agnostic_sql_append":[[4,2,1,"","SqlAppendOperator"]],"astro.sql.operators.agnostic_sql_append.SqlAppendOperator":[[4,3,1,"","append"],[4,3,1,"","execute"],[4,4,1,"","template_fields"]],"astro.sql.operators.agnostic_sql_merge":[[4,2,1,"","SqlMergeOperator"]],"astro.sql.operators.agnostic_sql_merge.SqlMergeOperator":[[4,3,1,"","execute"],[4,4,1,"","template_fields"]],"astro.sql.operators.agnostic_sql_truncate":[[4,2,1,"","SqlTruncateOperator"]],"astro.sql.operators.agnostic_sql_truncate.SqlTruncateOperator":[[4,3,1,"","execute"]],"astro.sql.operators.agnostic_stats_check":[[4,2,1,"","AgnosticStatsCheck"],[4,2,1,"","ChecksHandler"],[4,2,1,"","OutlierCheck"],[4,1,1,"","stats_check"]],"astro.sql.operators.agnostic_stats_check.AgnosticStatsCheck":[[4,3,1,"","execute"],[4,4,1,"","template_fields"]],"astro.sql.operators.agnostic_stats_check.ChecksHandler":[[4,3,1,"","check_results"],[4,3,1,"","evaluate_results"],[4,3,1,"","prepare_cases_sql"],[4,3,1,"","prepare_checks_sql"],[4,3,1,"","prepare_column_sql"],[4,3,1,"","prepare_comparison_sql"],[4,3,1,"","prepare_failed_checks_results"],[4,3,1,"","prepare_main_stats_sql"]],"astro.sql.operators.sql_dataframe":[[4,2,1,"","SqlDataframeOperator"]],"astro.sql.operators.sql_dataframe.SqlDataframeOperator":[[4,3,1,"","execute"],[4,3,1,"","get_snow_hook"],[4,3,1,"","handle_op_args"],[4,3,1,"","handle_op_kwargs"]],"astro.sql.operators.sql_decorator":[[4,2,1,"","SqlDecoratoratedOperator"],[4,1,1,"","transform_decorator"]],"astro.sql.operators.sql_decorator.SqlDecoratoratedOperator":[[4,3,1,"","convert_op_arg_dataframes"],[4,3,1,"","convert_op_kwarg_dataframes"],[4,3,1,"","create_cte"],[4,3,1,"","create_output_csv_path"],[4,3,1,"","create_temporary_table"],[4,3,1,"","default_transform"],[4,3,1,"","execute"],[4,3,1,"","get_bigquery_hook"],[4,3,1,"","get_postgres_hook"],[4,3,1,"","get_snow_hook"],[4,3,1,"","get_sql_alchemy_engine"],[4,3,1,"","get_sqlite_hook"],[4,3,1,"","handle_dataframe_func"],[4,3,1,"","handle_output_table_schema"],[4,3,1,"","handle_params"],[4,3,1,"","post_execute"],[4,3,1,"","pre_execute"],[4,3,1,"","read_sql"]],"astro.sql.table":[[3,2,1,"","Table"],[3,2,1,"","TempTable"],[3,1,1,"","create_table_name"]],"astro.sql.table.Table":[[3,3,1,"","identifier_args"],[3,3,1,"","qualified_name"],[3,4,1,"","template_fields"]],"astro.sql.table.TempTable":[[3,3,1,"","to_table"]],"astro.utils":[[5,0,0,"-","bigquery_merge_func"],[5,0,0,"-","cloud_storage_creds"],[5,0,0,"-","dependencies"],[5,0,0,"-","load_dataframe"],[5,0,0,"-","postgres_merge_func"],[5,0,0,"-","postgres_transform"],[5,0,0,"-","schema_util"],[5,0,0,"-","snowflake_append"],[5,0,0,"-","snowflake_merge_func"],[5,0,0,"-","snowflake_transform"],[5,0,0,"-","sqlite_merge_func"],[5,0,0,"-","table_handler"],[5,0,0,"-","task_id_helper"]],"astro.utils.bigquery_merge_func":[[5,1,1,"","bigquery_merge_func"]],"astro.utils.cloud_storage_creds":[[5,1,1,"","gcs_client"],[5,1,1,"","parse_s3_env_var"],[5,1,1,"","s3fs_creds"]],"astro.utils.dependencies":[[5,2,1,"","MissingPackage"]],"astro.utils.load_dataframe":[[5,1,1,"","move_dataframe_to_sql"]],"astro.utils.postgres_merge_func":[[5,1,1,"","postgres_merge_func"]],"astro.utils.postgres_transform":[[5,1,1,"","add_templates_to_context"],[5,1,1,"","create_sql_engine"]],"astro.utils.schema_util":[[5,1,1,"","create_schema_query"],[5,1,1,"","get_error_string_for_multiple_dbs"],[5,1,1,"","get_schema"],[5,1,1,"","get_table_name"],[5,1,1,"","schema_exists"],[5,1,1,"","tables_from_same_db"]],"astro.utils.snowflake_append":[[5,1,1,"","snowflake_append_func"]],"astro.utils.snowflake_merge_func":[[5,1,1,"","ensure_internal_quotes_closed"],[5,1,1,"","ensure_only_valid_characters"],[5,1,1,"","fill_in_append_statements"],[5,1,1,"","fill_in_merge_clauses"],[5,1,1,"","fill_in_update_statement"],[5,1,1,"","is_valid_snow_identifier"],[5,1,1,"","is_valid_snow_identifiers"],[5,1,1,"","snowflake_merge_func"],[5,1,1,"","wrap_identifier"]],"astro.utils.snowflake_transform":[[5,1,1,"","add_templates_to_context"],[5,1,1,"","process_params"]],"astro.utils.sqlite_merge_func":[[5,1,1,"","sqlite_merge_func"]],"astro.utils.table_handler":[[5,2,1,"","TableHandler"]],"astro.utils.table_handler.TableHandler":[[5,3,1,"","populate_output_table"]],"astro.utils.task_id_helper":[[5,1,1,"","get_task_id"]],astro:[[0,0,0,"-","constants"],[1,0,0,"-","dataframe"],[0,1,1,"","get_provider_info"],[2,0,0,"-","ml"],[3,0,0,"-","sql"],[5,0,0,"-","utils"]]},objnames:{"0":["py","module","Python module"],"1":["py","function","Python function"],"2":["py","class","Python class"],"3":["py","method","Python method"],"4":["py","attribute","Python attribute"]},objtypes:{"0":"py:module","1":"py:function","2":"py:class","3":"py:method","4":"py:attribute"},terms:{"0":[0,1,2,3,4,5,6],"100":4,"1000000":4,"2":[0,1,2,3,4,5,6],"boolean":[4,5],"class":[3,4,5],"default":4,"do":3,"float":4,"function":[1,2,4],"int":4,"new":3,"return":[3,4,5],"static":4,"true":4,A:3,AS:[0,1,2,3,4,5,6],For:2,IS:[0,1,2,3,4,5,6],In:4,OF:[0,1,2,3,4,5,6],OR:[0,1,2,3,4,5,6],The:[3,4,5],accepted_std_div:4,add:[2,4],add_templates_to_context:5,after:4,again:4,aggregate_check:4,agnostic_aggregate_check:[0,3],agnostic_boolean_check:[0,3],agnostic_load_fil:[0,3],agnostic_save_fil:[0,3],agnostic_sql_append:[0,3],agnostic_sql_merg:[0,3],agnostic_sql_trunc:[0,3],agnostic_stats_check:[0,3],agnostic_write_fil:4,agnosticaggregatecheck:4,agnosticbooleancheck:4,agnosticloadfil:4,agnosticstatscheck:4,agre:[0,1,2,3,4,5,6],airflow:[1,4,5],allow:[1,5],an:[0,1,2,3,4,5,6],ani:[0,1,2,3,4,5,6],apach:[0,1,2,3,4,5,6],append:[3,4],append_t:[3,4,5],applic:[0,1,2,3,4,5,6],ar:3,astronom:[0,1,2,3,4,5,6],attempt:5,autocommit:[3,4],automat:[1,4],bar:3,base:[3,4,5],baseoper:4,basi:[0,1,2,3,4,5,6],becaus:5,befor:4,belong:5,benefit:1,bigquery_merge_func:[0,7],bool:[1,2,3,4],boolean_check:4,call:4,callabl:[1,2,3,4],can:[1,5],casted_column:[3,4,5],check:[4,5],check_result:4,checkshandl:4,chunksiz:[4,5],cloud_storage_cr:[0,7],column:[3,4,5],columns_map:4,com:5,command:4,commit:4,compar:[3,4],compare_t:4,compare_table_sqla:4,complianc:[0,1,2,3,4,5,6],condit:[0,1,2,3,4,5,6],conflict:3,conflict_strategi:[3,4,5],conn:4,conn_id:[1,2,3,4,5],conn_typ:[4,5],connect:[3,4,5],constant:[6,7],content:7,context:[3,4,5],convert:4,convert_op_arg_datafram:4,convert_op_kwarg_datafram:4,copi:[0,1,2,3,4,5,6],copyright:[0,1,2,3,4,5,6],core:5,creat:4,create_ct:4,create_output_csv_path:4,create_schema_queri:5,create_sql_engin:5,create_table_nam:[3,4],create_temporary_t:4,credenti:5,csv:4,current:4,dag:4,databas:[1,2,3,4,5],datafram:[0,2,4,5,7],db:[4,5],decor:4,decoratedoper:4,default_transform:4,defin:4,depend:[0,7],deploi:2,deriv:4,determin:3,df:[4,5],dict:[3,4],dictionari:[3,4],distribut:[0,1,2,3,4,5,6],doc:5,doe:5,each:4,either:[0,1,2,3,4,5,6],en:5,enabl:5,engin:4,ensur:5,ensure_internal_quotes_clos:5,ensure_only_valid_charact:5,equal_to:4,equival:3,error:5,evaluate_result:4,eventu:2,ever:4,except:[0,1,2,3,4,5,6],execut:4,exist:4,expect:[4,5],experi:2,express:[0,1,2,3,4,5,6],fail:4,failed_check:4,fals:[3,4],featur:2,field:3,file:[0,1,2,3,4,5,6],file_conn_id:4,file_exist:4,fill_in_append_stat:5,fill_in_merge_claus:5,fill_in_update_stat:5,follow:5,foo:3,format:4,frame:5,from:[4,5],gc:[4,5],gcs_client:5,gener:5,get:5,get_bigquery_hook:4,get_error_string_for_multiple_db:5,get_express:4,get_failed_check:4,get_postgres_hook:4,get_provider_info:0,get_result:4,get_schema:5,get_snow_hook:4,get_sql_alchemy_engin:4,get_sqlite_hook:4,get_table_nam:5,get_task_id:5,get_template_context:4,govern:[0,1,2,3,4,5,6],greater_than:4,ha:4,handle_dataframe_func:4,handle_op_arg:4,handle_op_kwarg:4,handle_output_table_schema:4,handle_param:4,handler:[3,4],have:2,hook:[4,5],html:5,http:[0,1,2,3,4,5,6],huge:1,id:[3,4,5],identifi:5,identifier_arg:3,ignor:3,impli:[0,1,2,3,4,5,6],inc:[0,1,2,3,4,5,6],index:[4,6],infer:[2,4],inject:5,inp:5,input:4,input_conn_id:4,input_t:4,insert:[3,5],instanc:[3,4],instead:4,is_valid_snow_identifi:5,iter:[3,4],itself:4,jinja:4,kei:3,kind:[0,1,2,3,4,5,6],kwarg:[3,4],languag:[0,1,2,3,4,5,6],law:[0,1,2,3,4,5,6],length:3,less_than:4,licens:[0,1,2,3,4,5,6],limit:[0,1,2,3,4,5,6],list:[3,4,5],load:4,load_datafram:[0,7],load_fil:4,local:4,mai:[0,1,2,3,4,5,6],main:4,main_stat:4,main_tabl:[3,4,5],main_table_sqla:4,make:5,map:[3,4],matter:3,max:4,max_rows_return:4,meant:4,merg:3,merge_append_dict:5,merge_column:[3,4,5],merge_kei:[3,4,5],merge_t:[3,4,5],merge_target_dict:5,metadata_obj:4,method:[4,5],min:4,missingpackag:5,ml:[0,7],model:[2,4],modul:[6,7],module_nam:5,more:4,move_dataframe_to_sql:5,multipl:5,multiple_output:[1,2,3,4],name:[4,5],necessari:4,need:[3,5],none:[1,2,3,4],notat:4,now:2,number:4,object:[3,4,5],obtain:[0,1,2,3,4,5,6],one:[4,5],onli:4,oper:[0,3],optim:2,option:[1,2,3,4],order:3,org:[0,1,2,3,4,5,6],outliercheck:4,output:4,output_conn_id:4,output_file_format:4,output_file_path:4,output_t:[3,4],output_table_nam:[4,5],overwrit:[3,4],overwritten:4,packag:[6,7],page:6,panda:[4,5],param:[3,4,5],paramet:[3,4,5],parma:5,parquet:4,parse_s3_env_var:5,path:[4,5],perform:5,permiss:[0,1,2,3,4,5,6],populate_output_t:5,post_execut:4,postgr:4,postgres_conn_id:[4,5],postgres_merge_func:[0,7],postgres_transform:[0,7],pre_execut:4,predict:2,prefix:5,prep_boolean_checks_queri:4,prep_result:4,prepare_cases_sql:4,prepare_checks_sql:4,prepare_column_sql:4,prepare_comparison_sql:4,prepare_failed_checks_result:4,prepare_main_stats_sql:4,primari:3,process_param:5,provid:4,python:1,python_cal:[1,2,3,4],qualified_nam:3,queri:4,raw_sql:4,read_sql:4,reason:5,refer:[4,5],related_extra:5,render:4,repres:4,requir:[0,1,2,3,4,5,6],result:[1,4],right:4,role:[3,4],row:4,rtype:4,run:[1,4],run_raw_sql:3,s3:[4,5],s3f:5,s3fs_cred:5,same:[2,3,4,5],save_fil:4,savefil:4,schema:[1,2,3,4,5],schema_exist:5,schema_id:5,schema_util:[0,7],search:6,see:[0,1,2,3,4,5,6],select:4,self:4,session:4,set:4,snowflak:[3,4,5],snowflake_append:[0,7],snowflake_append_func:5,snowflake_conn_id:5,snowflake_merge_func:[0,7],snowflake_transform:[0,7],snowflakehook:4,softwar:[0,1,2,3,4,5,6],specif:[0,1,2,3,4,5,6],specifi:3,sql:[0,1,5,7],sql_datafram:[0,3],sql_decor:[0,3],sqlalchemi:4,sqlappendoper:4,sqldataframeoper:4,sqldecoratoratedoper:4,sqlite_merge_func:[0,7],sqlmergeoper:4,sqltruncateoper:4,statement:[4,5],stats_check:4,storag:5,str:[1,2,3,4,5],strategi:3,string:5,structur:5,submodul:[6,7],subpackag:[6,7],syntax:5,system:4,tabl:[0,4,5,7],table_handl:[0,4,7],table_nam:[3,4],tablehandl:[4,5],tables_from_same_db:5,targest_column:5,target_column:[3,4,5],target_t:[3,4,5],task:[4,5],task_id:4,task_id_help:[0,7],temp:4,temp_tabl:4,templat:4,template_field:[3,4],temporari:4,temptabl:[3,4],thi:[0,1,2,3,4,5,6],threshold:4,to_tabl:3,train:2,transform:3,transform_decor:4,transform_fil:3,trigger:4,truncat:3,turn:1,two:3,type:[3,4,5],under:[0,1,2,3,4,5,6],union:[3,4],uniqu:5,unless:[0,1,2,3,4,5,6],updat:[3,5],url:4,us:[0,1,2,3,4,5,6],user:[1,5],util:[0,4,7],valid:[4,5],validate_path:4,valu:[3,4],version:[0,1,2,3,4,5,6],want:3,warehous:[1,2,3,4,5],warranti:[0,1,2,3,4,5,6],we:[2,3,4,5],what:3,when:[3,4],which:4,without:[0,1,2,3,4,5,6],would:3,wrap_identifi:5,write:[0,1,2,3,4,5,6],www:[0,1,2,3,4,5,6],xcomarg:4,you:[0,1,2,3,4,5,6]},titles:["astro package","astro.dataframe package","astro.ml package","astro.sql package","astro.sql.operators package","astro.utils package","Welcome to Astro Projects\u2019s documentation!","astro"],titleterms:{agnostic_aggregate_check:4,agnostic_boolean_check:4,agnostic_load_fil:4,agnostic_save_fil:4,agnostic_sql_append:4,agnostic_sql_merg:4,agnostic_sql_trunc:4,agnostic_stats_check:4,astro:[0,1,2,3,4,5,6,7],bigquery_merge_func:5,cloud_storage_cr:5,constant:0,content:[0,1,2,3,4,5,6],datafram:1,depend:5,document:6,indic:6,load_datafram:5,ml:2,modul:[0,1,2,3,4,5],oper:4,packag:[0,1,2,3,4,5],postgres_merge_func:5,postgres_transform:5,project:6,s:6,schema_util:5,snowflake_append:5,snowflake_merge_func:5,snowflake_transform:5,sql:[3,4],sql_datafram:4,sql_decor:4,sqlite_merge_func:5,submodul:[0,3,4,5],subpackag:[0,3],tabl:[3,6],table_handl:5,task_id_help:5,util:5,welcom:6}})