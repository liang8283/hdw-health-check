#!/usr/bin/env python2
# -*- coding: utf-8 -*-

"""
@author: Chen Liang
"""

import subprocess
import sys
import os
import time
import codecs
import yaml
import re
import argparse
from datetime import datetime 
from pygresql import pgdb
from prettytable import PrettyTable

##################  SQL queries ################## 
get_db_version_sql = 'select version()'
get_db_names_sql = '''select datname from pg_database where datname not in ('template0','template1','postgres')'''
get_segment_config_sql = 'select dbid,content,role,preferred_role,mode,status,port,hostname,address from gp_segment_configuration order by dbid'
get_hosts_sql = 'select distinct(hostname) as hostname from gp_segment_configuration order by hostname'
get_guc_sql = '''
select name as name, setting as setting from pg_settings where name in (
'optimizer'
,'optimizer_analyze_root_partition'
,'log_statement'
,'checkpoint_segments'
,'gp_fts_probe_timeout'
,'gp_fts_probe_interval'
,'gp_segment_connect_timeout'
,'max_fsm_pages'
,'max_fsm_relations'
,'max_stack_depth'
,'max_appendonly_tables'
,'gp_external_max_segs'
,'gp_autostats_mode'
,'gp_autostats_on_change_threshold'
,'gp_filerep_tcp_keepalives_count'
,'gp_filerep_tcp_keepalives_interval'
,'gp_vmem_protect_limit'
,'statement_mem'
,'max_statement_mem'
,'gp_external_enable_exec'
,'statement_timeout'
,'gp_interconnect_type'
,'gp_analyze_relative_error'
,'default_statistics_target'
,'gp_workfile_limit_per_query'
,'superuser_reserved_connections'
,'gp_workfile_compress_algorithm'
,'max_connections'
,'max_prepared_transactions'
,'shared_buffers'
,'gpperfmon_log_alert_level'
,'join_collapse_limit')
'''
get_resqueue_sql = 'SELECT * FROM gp_toolkit.gp_resqueue_status'
check_standby_sql_pg9 = 'SELECT pid, state FROM pg_stat_replication'
check_standby_sql_pg8 = 'SELECT procpid, state FROM pg_stat_replication'
get_master_log_sql = '''select logtime,loguser,logdatabase,logpid,loghost,logsessiontime,logsession,logsegment,logseverity,logmessage,logquery 
    from gp_toolkit.gp_log_system
    where logseverity in ('FATAL','PANIC')
    and logtime > now() - interval '7 day'
    order by logtime desc limit 100
    '''
get_pg_activity_sql_pg9 = '''
select datname,pid,sess_id,usename,application_name,client_addr,client_hostname,backend_start,xact_start,query_start,date_part('second', now()-query_start) as duration_sec,waiting,state,query,waiting_reason,rsgname,rsgqueueduration 
from pg_stat_activity
where date_part('second', now()-query_start) > 3600
'''
get_pg_activity_sql_pg8 = '''
select datname,procpid,sess_id,usename,application_name,client_addr,backend_start,xact_start,query_start,date_part('second', now()-query_start) as duration_sec,waiting,current_query,waiting_reason,rsgname,rsgqueueduration 
from pg_stat_activity
where date_part('second', now()-query_start) > 3600
'''
get_pg_locks_sql_pg9 = '''
select a.gp_segment_id, a.pid, a.mode, a.mppsessionid, c.nspname,b.relname, date_part('second', now()-d.query_start) as lock_duration_sec, d.query as query_hold_lock
from pg_locks a, pg_class b, pg_namespace c, pg_stat_activity d
where a.relation=b.oid and b.relnamespace=c.oid
and a.locktype='relation' and granted = 't' 
and a.mppsessionid = d.sess_id
and date_part('second', now()-d.query_start) > 600
and relation in (select relation from pg_locks where granted = 'f')
order by gp_segment_id
'''
get_pg_locks_sql_pg8 = '''
select a.gp_segment_id, a.pid, a.mode, a.mppsessionid, c.nspname,b.relname, date_part('second', now()-d.query_start) as lock_duration_sec, d.current_query as query_hold_lock
from pg_locks a, pg_class b, pg_namespace c, pg_stat_activity d
where a.relation=b.oid and b.relnamespace=c.oid
and a.locktype='relation' and granted = 't' 
and a.mppsessionid = d.sess_id
and date_part('second', now()-d.query_start) > 600
and relation in (select relation from pg_locks where granted = 'f')
order by gp_segment_id
'''
get_bloat_sql = 'select * from gp_toolkit.gp_bloat_diag where bdirelpages/bdiexppages >=5 order by bdirelpages/bdiexppages desc limit 20'
get_ao_bloat_sql = '''
select * from (
SELECT 
c.oid, 
n.nspname AS schema_name, 
c.relname AS table_name, 
c.reltuples::bigint AS num_rows, 
(SELECT max(percent_hidden) FROM gp_toolkit.__gp_aovisimap_compaction_info(c.oid)) as percent_hidden, 
(SELECT sum(total_tupcount) FROM gp_toolkit.__gp_aovisimap_compaction_info(c.oid)) as total_tupcount, 
(SELECT sum(hidden_tupcount) FROM gp_toolkit.__gp_aovisimap_compaction_info(c.oid))  as hidden_tupcount 
FROM pg_appendonly a 
JOIN pg_class c ON c.oid=a.relid 
JOIN pg_namespace n ON c.relnamespace=n.oid 
WHERE relstorage in ('c', 'a')
and c.reltuples > 100000
) as ao_bloat
where percent_hidden > 20
'''
get_diskspace_sql = '''
SELECT distinct dfhostname, dfdevice, (dfspace/1024/1024)::decimal(18,2) as "space_avail_gb" FROM gp_toolkit.gp_disk_free order by dfhostname
'''
get_db_size_sql = '''select sodddatname as "db_name",(sodddatsize/1024/1024)::decimal(18,2) as "db_size_mb" from gp_toolkit.gp_size_of_database order by db_size_mb desc'''
get_schema_size_sql = '''select sosdnsp as "schema_name", (sosdschematablesize/1024/1024)::decimal(18,2) as "schema_tables_size_mb" from gp_toolkit.gp_size_of_schema_disk order by "schema_tables_size_mb" desc'''
#get_table_size_sql = '''
#    SELECT pg_namespace.nspname AS schema, relname AS name, (sotdsize/1024/1024)::decimal(18,2) AS size_mb
#    FROM gp_toolkit.gp_size_of_table_disk as sotd, pg_class , pg_namespace
#    WHERE sotd.sotdoid=pg_class.oid 
#    and pg_class.relnamespace = pg_namespace.oid
#    ORDER BY size_mb desc limit 20
#    '''
get_table_size_sql = '''
select  schemaname,
        relname,
        (sum(q.table_size)/1024/1024)::decimal(18,2) as size_mb
    from (
        select  coalesce(p.schemaname, n.nspname) as schemaname,
                coalesce(p.tablename, c.relname) as relname,
                pg_relation_size(quote_ident(n.nspname) || '.' || quote_ident(c.relname)) as table_size
            from pg_class as c
                inner join pg_namespace as n on c.relnamespace = n.oid
                left join pg_partitions as p on c.relname = p.partitiontablename and n.nspname = p.partitionschemaname    
            WHERE n.nspname not in ('information_schema','gp_toolkit','pg_toast')
        ) as q
    group by 1, 2
    order by sum(q.table_size) desc
    limit 10
'''
create_data_skew_fn_sql = """
CREATE OR REPLACE FUNCTION public.fn_get_skew(out schema_name      varchar,
                                              out table_name       varchar,
                                              out pTableName       varchar,
                                              out total_size_GB    numeric(15,2),
                                              out seg_min_size_GB  numeric(15,2),
                                              out seg_max_size_GB  numeric(15,2),
                                              out seg_avg_size_GB  numeric(15,2),
                                              out seg_gap_min_max_percent numeric(6,2),
                                              out seg_gap_min_max_GB      numeric(15,2),
                                              out nb_empty_seg     int) RETURNS SETOF record AS
$$
DECLARE
    v_function_name text := 'fn_get_skew';
    v_location int;
    v_sql text;
    v_db_oid text;
    v_num_segments numeric;
    v_skew_amount numeric;
    v_res record;
BEGIN
    v_location := 1000;
    SELECT oid INTO v_db_oid
    FROM pg_database
    WHERE datname = current_database();

    v_location := 2200;
    v_sql := 'DROP EXTERNAL TABLE IF EXISTS public.db_files_ext';

    v_location := 2300;
    EXECUTE v_sql;

    v_location := 3000;
    v_sql := 'CREATE EXTERNAL WEB TABLE public.db_files_ext ' ||
            '(segment_id int, relfilenode text, filename text, ' ||
            'size numeric) ' ||
            'execute E''ls -l $GP_SEG_DATADIR/base/' || v_db_oid ||
            ' | ' ||
            'grep gpadmin | ' ||
            E'awk {''''print ENVIRON["GP_SEGMENT_ID"] "\\t" $9 "\\t" ' ||
            'ENVIRON["GP_SEG_DATADIR"] "/' || v_db_oid ||
            E'/" $9 "\\t" $5''''}'' on all ' || 'format ''text''';

    v_location := 3100;
    EXECUTE v_sql;

    v_location := 4000;
    for v_res in (
                select  sub.vschema_name,
                        sub.vtable_name,
                        (sum(sub.size)/(1024^3))::numeric(15,2) AS vtotal_size_GB,
                        --Size on segments
                        (min(sub.size)/(1024^3))::numeric(15,2) as vseg_min_size_GB,
                        (max(sub.size)/(1024^3))::numeric(15,2) as vseg_max_size_GB,
                        (avg(sub.size)/(1024^3))::numeric(15,2) as vseg_avg_size_GB,
                        --Percentage of gap between smaller segment and bigger segment
                        (100*(max(sub.size) - min(sub.size))/greatest(max(sub.size),1))::numeric(6,2) as vseg_gap_min_max_percent,
                        ((max(sub.size) - min(sub.size))/(1024^3))::numeric(15,2) as vseg_gap_min_max_GB,
                        count(sub.size) filter (where sub.size = 0) as vnb_empty_seg
                    from (
                        SELECT  n.nspname AS vschema_name,
                                c.relname AS vtable_name,
                                db.segment_id,
                                sum(db.size) AS size
                            FROM ONLY public.db_files_ext db
                                JOIN pg_class c ON split_part(db.relfilenode, '.'::text, 1) = c.relfilenode::text
                                JOIN pg_namespace n ON c.relnamespace = n.oid
                            WHERE c.relkind = 'r'::"char"
                                and n.nspname not in ('pg_catalog','information_schema','gp_toolkit')
                                and not n.nspname like 'pg_temp%'
                            GROUP BY n.nspname, c.relname, db.segment_id
                        ) sub
                    group by 1,2
                    --Extract only table bigger than 1 GB
                    --   and with a skew greater than 20%
                    having sum(sub.size)/(1024^3) > 1
                        and (100*(max(sub.size) - min(sub.size))/greatest(max(sub.size),1))::numeric(6,2) > 20
                    order by vtotal_size_GB desc, vseg_gap_min_max_percent desc
                    limit 100 ) loop
        schema_name         = v_res.vschema_name;
        table_name          = v_res.vtable_name;
        total_size_GB       = v_res.vtotal_size_GB;
        seg_min_size_GB     = v_res.vseg_min_size_GB;
        seg_max_size_GB     = v_res.vseg_max_size_GB;
        seg_avg_size_GB     = v_res.vseg_avg_size_GB;
        seg_gap_min_max_percent = v_res.vseg_gap_min_max_percent;
        seg_gap_min_max_GB  = v_res.vseg_gap_min_max_GB;
        nb_empty_seg        = v_res.vnb_empty_seg;
        return next;
    end loop;

    v_location := 4100;
    v_sql := 'DROP EXTERNAL TABLE IF EXISTS public.db_files_ext';

    v_location := 4200;
    EXECUTE v_sql;

    return;
EXCEPTION
        WHEN OTHERS THEN
                RAISE EXCEPTION '(%:%:%)', v_function_name, v_location, sqlerrm;
END;
$$
language plpgsql;
    """
get_data_skew_sql = 'select * from public.fn_get_skew()'
get_db_age_sql = '''
WITH cluster AS (
	SELECT gp_segment_id, datname, age(datfrozenxid) age FROM pg_database
	UNION ALL
	SELECT gp_segment_id, datname, age(datfrozenxid) age FROM gp_dist_random('pg_database')
)
SELECT  gp_segment_id, datname, age,
    CASE
            WHEN age < (2^31-1 - current_setting('xid_stop_limit')::int - current_setting('xid_warn_limit')::int) THEN 'BELOW WARN LIMIT'
            WHEN  ((2^31-1 - current_setting('xid_stop_limit')::int - current_setting('xid_warn_limit')::int) < age) AND (age <  (2^31-1 - current_setting('xid_stop_limit')::int)) THEN 'OVER WARN LIMIT and UNDER STOP LIMIT'
            WHEN age > (2^31-1 - current_setting('xid_stop_limit')::int ) THEN 'OVER STOP LIMIT'
            WHEN age < 0 THEN 'OVER WRAPAROUND'
    END
FROM cluster
ORDER BY datname, gp_segment_id
'''
get_table_age_sql = '''
select gp_segment_id, table_name, age,
    CASE
            WHEN age < (2^31-1 - current_setting('xid_stop_limit')::int - current_setting('xid_warn_limit')::int) THEN 'BELOW WARN LIMIT'
            WHEN  ((2^31-1 - current_setting('xid_stop_limit')::int - current_setting('xid_warn_limit')::int) < age) AND (age <  (2^31-1 - current_setting('xid_stop_limit')::int)) THEN 'OVER WARN LIMIT and UNDER STOP LIMIT'
            WHEN age > (2^31-1 - current_setting('xid_stop_limit')::int ) THEN 'OVER STOP LIMIT'
            WHEN age < 0 THEN 'OVER WRAPAROUND'
    END
from 
(select aa.gp_segment_id,bb.nspname||'.'||aa.relname as "table_name",age(relfrozenxid) age, 
ROW_NUMBER() over (partition by aa.gp_segment_id order by age(relfrozenxid) desc) as pos
from gp_dist_random('pg_class') aa, pg_namespace bb
where aa.relnamespace=bb.oid and relkind='r' and relstorage!='x' 
and bb.nspname not in ('information_schema')
and age(relfrozenxid) != 2147483647
and aa.relname not like '%persistent%' and aa.relname not like '%gp_global%') ss
where pos <= 3
order by gp_segment_id, age desc
'''
get_temp_schema_sql = '''
select nspname from pg_namespace where nspname like 'pg_temp%' except select 'pg_temp_' || sess_id::varchar from pg_stat_activity
union
select nspname from gp_dist_random('pg_namespace') where nspname like 'pg_temp%' except select 'pg_temp_' || sess_id::varchar from pg_stat_activity
'''
get_stale_stats_sql = '''
select a.schemaname,a.relname,last_vacuum,last_analyze,last_autoanalyze
from pg_stat_all_tables a join pg_class b
on a.relid = b.oid
where a.schemaname not in ('pg_toast','pg_catalog','information_schema','gp_toolkit')
and a.schemaname !~ '^pg_toast'
and a.relname not like '%prt%'
and b.reltuples > 10000
and COALESCE(last_vacuum,'2022-01-01',last_vacuum) < now() - interval '7 day' 
and COALESCE(last_analyze,'2022-01-01',last_analyze) < now() - interval '7 day'
and COALESCE(last_autoanalyze,'2022-01-01',last_analyze) < now() - interval '7 day'
order by a.schemaname, a.relname
'''
create_sp_gp_skew_sql = '''
set SEARCH_PATH='gp_toolkit';
CREATE OR REPLACE FUNCTION gp_toolkit.sp_gp_skew_coefficients(schemanm varchar(200), tablename varchar(300)) RETURNS SETOF gp_skew_analysis_t
 	AS $$
DECLARE
 	skcoid oid;
 	skcrec record;
BEGIN
 	SELECT autoid INTO skcoid 
 	FROM
		gp_toolkit.__gp_user_data_tables_readable 
 	WHERE autrelstorage not in ('x','v') 
	AND autnspname = schemanm 
	AND autrelname = tablename;
 	
 	SELECT * INTO skcrec
 	FROM
 		gp_toolkit.gp_skew_coefficient(skcoid); 
 	RETURN next skcrec;
END
 	$$
LANGUAGE plpgsql;
    '''
get_ao_table_list_sql = '''
select n.nspname, c.relname
from pg_namespace n join pg_class c
on n.oid = c.relnamespace
where n.nspname not in ('information_schema','pg_catalog','gp_toolkit','pg_toast','pg_aoseg') 
and relstorage in ('a','c')
and c.reltuples > 100000
'''
get_ao_data_skew_sql = '''
select 
skew.skewoid AS skcoid, 
pgn.nspname AS skcnamespace, 
pgc.relname AS skcrelname, 
skew.skewval AS skccoeff 
from 
	gp_toolkit.sp_gp_skew_coefficients(%s,%s) skew(skewoid, skewval) 
JOIN pg_class pgc ON skew.skewoid = pgc.oid 
JOIN pg_namespace pgn ON pgc.relnamespace = pgn.oid
'''
################## Common functions ################## 
def execSQL(conn,sql,params=''):
    cursor=conn.cursor()
    cursor.execute(sql,params)
    return cursor

def get_hosts_list(dbconn):
    hosts = execSQL(dbconn,get_hosts_sql)
    hosts_list = [row[0] for row in hosts]
    return hosts_list

def get_db_list(dbconn):
    cursor = execSQL(dbconn,get_db_names_sql)
    db_names_list = cursor.fetchall()
    return [row[0] for row in db_names_list]

def get_pg_version(dbconn):
    pg_kernal = ''
    cursor = execSQL(dbconn, 'select version()')
    pg_version = cursor.fetchone()
    if 'HashData Warehouse 3' in pg_version[0]:
        pg_kernal = 'hdw3'
    else:
        if 'PostgreSQL 9' in pg_version[0]:
            pg_kernal = 'pg9'
        elif 'PostgreSQL 8' in pg_version[0]:
            pg_kernal = 'pg8'
    return pg_kernal

def _execute_shell_command(bash_command):
    try:
        output = subprocess.check_output(bash_command, shell=True).decode().rstrip()
    except subprocess.CalledProcessError as e:
        output = str(e)
    return output

def check_items_output(check_item, check_result, check_result_detail, rpt_format):
    green_print_flag = '\033[1;32m'
    red_print_flag = '\033[1;31m'
    color_print_end_flag = '\033[0m'
    html_color = ''
    color_print_start_flag = ''
    if check_result == 'OK':
        color_print_start_flag = green_print_flag
        html_color = 'green'
    if 'NOT OK' in check_result:
        color_print_start_flag = red_print_flag
        html_color = 'red'
    if rpt_format == 'text':
        check_details_output = '\n\n### Check: ' + check_item + '\n\n' + color_print_start_flag + 'Result:\n    ' + check_result + color_print_end_flag + '\n\nDetails:\n'
        check_result_detail_indent_list = ['    ' + line for line in check_result_detail.splitlines()]
        check_result_detail_indent = '\n'.join(check_result_detail_indent_list)
        check_details_output += check_result_detail_indent
    if rpt_format == 'html':
        check_details_output = '''
        <div style="clear:both">
            <p>
            <br>
            <h3 style="text-align:left; margin:0; padding:0;">Check: %s</h3>
            <font color=%s><b>Result: %s</b></font>
            <br>
            <b>Details:</b>
            <br> 
            %s
            <br>
            </p>
        </div>
        ''' % (check_item,html_color,check_result,check_result_detail)
    return check_details_output

################## Health check items ################## 
def get_db_version(dbconn,rpt_format):
    check_item = 'Database Version'
    check_result = 'OK'
    check_result_detail = ''
    cursor = execSQL(dbconn, get_db_version_sql)
    db_version_result = cursor.fetchone()
    db_version = db_version_result[0].split('on')[0]
    db_version_table = PrettyTable(['DB Version'])
    db_version_table.add_row([db_version])
    if rpt_format == 'text': 
        check_result_detail = db_version_table.get_string()
    if rpt_format == 'html':
        check_result_detail = db_version_table.get_html_string(attributes={
            'width': '60%',
            'align': 'left',
            'BORDERCOLOR': '#330000',
            'border': 2,
        })
    db_version_output = check_items_output(check_item, check_result, check_result_detail, rpt_format)
    return (check_item, check_result, db_version_output)

def seg_config_check(dbconn,rpt_format):
    check_item = 'Cluster Configuration'
    check_result = 'OK'
    check_result_detail = ''
    cursor = execSQL(dbconn,get_segment_config_sql)
    seg_configs = cursor.fetchall()
    column_names_list = [row[0] for row in cursor.description]
    check_result_table = PrettyTable(column_names_list)
    for row in seg_configs:
        check_result_table.add_row(row)
    if rpt_format == 'text': 
        check_result_detail = check_result_table.get_string()
    if rpt_format == 'html':
        check_result_detail = check_result_table.get_html_string(attributes={
            'width': '60%',
            'align': 'left',
            'BORDERCOLOR': '#330000',
            'border': 2,
        })
    seg_configs_check_output = check_items_output(check_item, check_result, check_result_detail, rpt_format)
    return (check_item, check_result, seg_configs_check_output)

def os_version_check(hosts_list,rpt_format):
    check_item = 'OS Version'
    check_result = 'OK'
    os_version_check_list= []
    check_result_table = PrettyTable(["Host","OS version"])
    for host in hosts_list:
        os_version_cmd = 'ssh gpadmin@%s "cat /etc/os-release | grep PRETTY_NAME | cut -f2 -d ="' % (host)
        os_version_output = _execute_shell_command(os_version_cmd)
        os_version_check_list.append(os_version_output)
        check_result_table.add_row([host,os_version_output])
    if len(set(os_version_check_list)) != 1:
        check_result = 'NOT OK'
    if rpt_format == 'text': 
        check_result_detail = check_result_table.get_string()
    if rpt_format == 'html':
        check_result_detail = check_result_table.get_html_string(attributes={
            'width': '60%',
            'align': 'left',
            'BORDERCOLOR': '#330000',
            'border': 2,
        })
    os_version_check_output = check_items_output(check_item, check_result, check_result_detail, rpt_format)
    return (check_item, check_result, os_version_check_output)   

def cpu_cores_check(hosts_list,rpt_format):
    check_item = 'CPU Cores'
    check_result = 'OK'
    cpu_cores_check_list= []
    check_result_table = PrettyTable(["Host","CPU Cores"])
    for host in hosts_list:
        cpu_cores_cmd = 'ssh gpadmin@%s "cat /proc/cpuinfo| grep "processor"| wc -l"' % (host)
        cpu_cores_output = _execute_shell_command(cpu_cores_cmd)
        cpu_cores_check_list.append(cpu_cores_output)
        check_result_table.add_row([host,cpu_cores_output])
    if len(set(cpu_cores_check_list)) != 1:
        check_result = 'NOT OK'
    if rpt_format == 'text': 
        check_result_detail = check_result_table.get_string()
    if rpt_format == 'html':
        check_result_detail = check_result_table.get_html_string(attributes={
            'width': '60%',
            'align': 'left',
            'BORDERCOLOR': '#330000',
            'border': 2,
        })
    cpu_cores_check_output = check_items_output(check_item, check_result, check_result_detail, rpt_format)
    return (check_item, check_result, cpu_cores_check_output)   

def memory_size_check(hosts_list,rpt_format):
    check_item = 'Memory Size'
    check_result = 'OK'
    memory_size_check_list= []
    check_result_table = PrettyTable(["Host","Memory Size"])
    for host in hosts_list:
        memory_size_check_cmd = 'ssh gpadmin@%s "free -g" | grep Mem | awk \'{print $2}\'' % (host)
        memory_size_check_output = _execute_shell_command(memory_size_check_cmd)
        memory_size_check_list.append(memory_size_check_output)
        check_result_table.add_row([host,memory_size_check_output + 'GB'])
    if len(set(memory_size_check_list)) != 1:
        check_result = 'NOT OK'
    if rpt_format == 'text': 
        check_result_detail = check_result_table.get_string()
    if rpt_format == 'html':
        check_result_detail = check_result_table.get_html_string(attributes={
            'width': '60%',
            'align': 'left',
            'BORDERCOLOR': '#330000',
            'border': 2,
        })
    memory_size_check_output = check_items_output(check_item, check_result, check_result_detail, rpt_format)
    return (check_item, check_result, memory_size_check_output)   

def diskspace_check(dbconn,rpt_format):
    check_item = 'Disk Space'
    check_result = 'OK'
    check_result_detail = ''
    cursor = execSQL(dbconn,get_diskspace_sql)
    diskspace_result = cursor.fetchall()
    column_names_list = [row[0] for row in cursor.description]
    check_result_table = PrettyTable(column_names_list)
    for row in diskspace_result:
        if row[-1] < 10:
            check_result = 'NOT OK'
        check_result_table.add_row(row)
    if rpt_format == 'text': 
        check_result_detail = check_result_table.get_string()
    if rpt_format == 'html':
        check_result_detail = check_result_table.get_html_string(attributes={
            'width': '60%',
            'align': 'left',
            'BORDERCOLOR': '#330000',
            'border': 2,
        })
    diskspace_check_output = check_items_output(check_item, check_result, check_result_detail, rpt_format)
    return (check_item, check_result, diskspace_check_output)

def host_load_check(hosts_list,rpt_format):
    check_item = 'Hosts Load'
    check_result = 'OK'
    check_result_detail = ''
    all_hosts_uptime_list = []
    for host in hosts_list:
        cpu_cores_cmd = 'ssh gpadmin@%s "cat /proc/cpuinfo| grep "processor"| wc -l"' % (host)
        cpu_cores_output = _execute_shell_command(cpu_cores_cmd)
        uptime_cmd = 'ssh gpadmin@%s "uptime"' % (host)
        uptime_output = _execute_shell_command(uptime_cmd).replace('\n', '') + '\n'
        uptime_output_list = [host] + uptime_output.split(',')
        if uptime_output_list[-1] > cpu_cores_output:
            check_result = 'NOT OK'
        all_hosts_uptime_list.append(uptime_output_list)
    all_hosts_uptime_list.sort(key=lambda x: x[-1], reverse=True)
    hosts_load_table = PrettyTable(['host','load'])
    for host in all_hosts_uptime_list:
        hosts_load_table.add_row([host[0],','.join(host[1:])])
    if rpt_format == 'text': 
        check_result_detail = hosts_load_table.get_string()
    if rpt_format == 'html':
        check_result_detail = hosts_load_table.get_html_string(attributes={
            'width': '60%',
            'align': 'left',
            'BORDERCOLOR': '#330000',
            'border': 2,
        })
    host_load_check_output = check_items_output(check_item, check_result, check_result_detail, rpt_format)
    return (check_item, check_result, host_load_check_output)   

def segments_check(rpt_format):
    check_item = 'Segment Status'
    check_result = 'OK'
    check_result_detail = ''
    gpstate_cmd = 'gpstate -e || true'
    gpstate_output = _execute_shell_command(gpstate_cmd)
    gpstate_output_list = [line.split(':')[-1] for line in gpstate_output.splitlines()]
    gpstate_output_start_line =  gpstate_output_list.index('-Segment Mirroring Status Report')
    check_result_detail = '\n'.join(gpstate_output_list[gpstate_output_start_line-1:])
    if rpt_format == 'html':
        check_result_detail = '<br>'.join(gpstate_output_list[gpstate_output_start_line-1:])
    if 'All segments are running normally' not in check_result_detail:
        check_result = 'NOT OK'
    gpstate_check_output = check_items_output(check_item, check_result, check_result_detail, rpt_format)
    return (check_item, check_result, gpstate_check_output)

def standby_check(dbconn, pg_version, rpt_format):
    check_item = 'Standby Master'
    check_result = 'OK'
    check_result_detail = ''
    if pg_version == 'pg9':
        cursor = execSQL(dbconn, check_standby_sql_pg9)
    if pg_version == 'pg8':
        cursor = execSQL(dbconn, check_standby_sql_pg8)
    standby_output = cursor.fetchone()
    column_names_list = [row[0] for row in cursor.description]
    check_result_table = PrettyTable(column_names_list)
    if cursor.rowcount == 1:
        check_result_table.add_row(standby_output)
        if standby_output[1] != 'streaming':
            check_result = 'NOT OK'
    else:
        check_result = 'NOT OK' 
    if rpt_format == 'text': 
        check_result_detail = check_result_table.get_string()
    if rpt_format == 'html':
        check_result_detail = check_result_table.get_html_string(attributes={
            'width': '60%',
            'align': 'left',
            'BORDERCOLOR': '#330000',
            'border': 2,
        })
    standby_check_output = check_items_output(check_item, check_result, check_result_detail, rpt_format)
    return (check_item,check_result,standby_check_output)

def guc_check(dbconn,rpt_format):
    check_item = 'Database Parameters'
    check_result = 'OK'
    check_result_detail = ''
    cursor = execSQL(dbconn,get_guc_sql)
    get_guc_result = cursor.fetchall()
    column_names_list = [row[0] for row in cursor.description]
    check_result_table = PrettyTable(column_names_list)
    for row in get_guc_result:
        check_result_table.add_row(row)
    if rpt_format == 'text': 
        check_result_detail = check_result_table.get_string()
    if rpt_format == 'html':
        check_result_detail = check_result_table.get_html_string(attributes={
            'width': '60%',
            'align': 'left',
            'BORDERCOLOR': '#330000',
            'border': 2,
        })
    guc_check_output = check_items_output(check_item, check_result, check_result_detail, rpt_format)
    return (check_item, check_result, guc_check_output)

def db_size_check(dbconn, rpt_format):
    check_item = 'Database Size'
    check_result = 'OK'
    check_result_detail = ''
    cursor = execSQL(dbconn,get_db_size_sql)
    db_size_result = cursor.fetchall()
    column_names_list = [row[0] for row in cursor.description]
    check_result_table = PrettyTable(column_names_list)
    for row in db_size_result:
        check_result_table.add_row(row)
    if rpt_format == 'text': 
        check_result_detail = check_result_table.get_string()
    if rpt_format == 'html':
        check_result_detail = check_result_table.get_html_string(attributes={
            'width': '60%',
            'align': 'left',
            'BORDERCOLOR': '#330000',
            'border': 2,
        })
    db_size_output = check_items_output(check_item, check_result, check_result_detail, rpt_format)
    return (check_item, check_result, db_size_output)

def schema_size_check(db_list,rpt_format):
    check_item = 'Schema Size'
    check_result = 'OK'
    check_result_detail = ''
    for db in db_list:
        dbconn = pgdb.connect(database=db, host='localhost:5432', user='gpadmin')
        cursor = execSQL(dbconn,get_schema_size_sql)
        schema_size_result = cursor.fetchall()
        column_names_list = [row[0] for row in cursor.description]
        check_result_table = PrettyTable(column_names_list)
        for row in schema_size_result:
            check_result_table.add_row(row)
        if rpt_format == 'text': 
            check_result_detail += '\nDatabase: ' + db + '\n' + check_result_table.get_string() + '\n'
        if rpt_format == 'html':
            check_result_detail += '<div style="clear:both"><br><b><li>Database: ' + db + '</li></b><div style="clear:both">\n' + check_result_table.get_html_string(attributes={
            'width': '60%',
            'align': 'left',
            'BORDERCOLOR': '#330000',
            'border': 2,
        }) + '\n<br>'
        dbconn.close()
    schema_size_output = check_items_output(check_item, check_result, check_result_detail, rpt_format)
    return (check_item, check_result, schema_size_output)

def table_size_check(db_list,rpt_format):
    check_item = 'Tables Size'
    check_result = 'OK'
    check_result_detail = ''
    for db in db_list:
        dbconn = pgdb.connect(database=db, host='localhost:5432', user='gpadmin')
        cursor = execSQL(dbconn,get_table_size_sql)
        table_size_result = cursor.fetchall()
        column_names_list = [row[0] for row in cursor.description]
        check_result_table = PrettyTable(column_names_list)
        for row in table_size_result:
            check_result_table.add_row(row)
        if rpt_format == 'text': 
            check_result_detail += '\nDatabase: ' + db + '\n' + check_result_table.get_string() + '\n'
        if rpt_format == 'html':
            check_result_detail += '<div style="clear:both"><br><b><li>Database: ' + db + '</li></b><div style="clear:both">\n' + check_result_table.get_html_string(attributes={
            'width': '60%',
            'align': 'left',
            'BORDERCOLOR': '#330000',
            'border': 2,
        }) + '\n<br>'
        dbconn.close()
    table_size_output = check_items_output(check_item, check_result, check_result_detail, rpt_format)
    return (check_item, check_result, table_size_output)

def data_skew_check(db_list,pg_version,rpt_format):
    check_item = 'Tables Data Skew'
    check_result = 'OK'
    if pg_version != 'hdw3': 
        check_result_detail = 'See below details for tables > 20% data skew.\n'
        for db in db_list:
            dbconn = pgdb.connect(database=db, host='localhost:5432', user='gpadmin')
            create_function_output = execSQL(dbconn,create_data_skew_fn_sql)
            cursor = execSQL(dbconn,get_data_skew_sql)
            get_data_skew_result = cursor.fetchall()
            column_names_list = [row[0] for row in cursor.description]
            check_result_table = PrettyTable(column_names_list)
            if cursor.rowcount >= 1:
                check_result = 'NOT OK'
                for row in get_data_skew_result:
                    check_result_table.add_row(row)
            dbconn.close()  
    if pg_version == 'hdw3':
        check_result_detail = 'See below details for tables with greatest data skew.\n\n'
        for db in db_list:
            dbconn = pgdb.connect(database=db, host='localhost:5432', user='gpadmin')
            execSQL(dbconn,create_sp_gp_skew_sql)
            cursor = execSQL(dbconn,get_ao_table_list_sql)
            table_list = cursor.fetchall()
            check_result_table = PrettyTable(['oid','schema','table','skccoeff'])
            for table in table_list:
                schema_name = table[0]
                table_name = table[1]
                cursor = execSQL(dbconn, get_ao_data_skew_sql, (schema_name,table_name,))
                skew_result = cursor.fetchone()
                skccoeff = skew_result[-1]
                if skccoeff > 15:
                    check_result = 'NOT OK.'
                    check_result_table.add_row(skew_result)
            check_result_table.sortby = "skccoeff"
            check_result_table.reversesort = True
            dbconn.close()  
    if rpt_format == 'text': 
        check_result_detail += '\nDatabase: ' + db + '\n' + check_result_table.get_string() + '\n'
    if rpt_format == 'html':
        check_result_detail += '<div style="clear:both"><br><b><li>Database: ' + db + '</li></b><div style="clear:both">\n' + check_result_table.get_html_string(attributes={
        'width': '60%',
        'align': 'left',
        'BORDERCOLOR': '#330000',
        'border': 2,
    }) + '\n<br>'   
    data_skew_output = check_items_output(check_item, check_result, check_result_detail, rpt_format)
    return (check_item, check_result, data_skew_output)

def resqueue_check(dbconn,rpt_format):
    check_item = 'Resource Queues Setting'
    check_result = 'OK'
    check_result_detail = ''
    cursor = execSQL(dbconn,get_resqueue_sql)
    resqueues = cursor.fetchall()
    column_names_list = [row[0] for row in cursor.description]
    check_result_table = PrettyTable(column_names_list)
    for row in resqueues:
        check_result_table.add_row(row)
    if rpt_format == 'text': 
        check_result_detail = check_result_table.get_string()
    if rpt_format == 'html':
        check_result_detail = check_result_table.get_html_string(attributes={
            'width': '60%',
            'align': 'left',
            'BORDERCOLOR': '#330000',
            'border': 2,
        })
    if cursor.rowcount == 1:
        check_result = 'NOT OK'
    resqueues_check_output = check_items_output(check_item, check_result, check_result_detail, rpt_format)
    return (check_item, check_result, resqueues_check_output)

def pg_activity_check(dbconn, pg_version, rpt_format):
    check_item = 'Current Long Running(> 1hr) Queries'
    check_result = 'OK'
    check_result_detail = ''
    if pg_version == 'pg9' or pg_version == 'hdw3':
        cursor = execSQL(dbconn, get_pg_activity_sql_pg9)
    if pg_version == 'pg8':
        cursor = execSQL(dbconn, get_pg_activity_sql_pg8)
    pg_activity_result = cursor.fetchall()
    column_names_list = [row[0] for row in cursor.description]
    pg_activity_table = PrettyTable(column_names_list)
    if cursor.rowcount > 0:
        check_result = 'NOT OK'
        for row in pg_activity_result:
            pg_activity_table.add_row(row)
    if rpt_format == 'text': 
        check_result_detail = pg_activity_table.get_string()
    if rpt_format == 'html':
        check_result_detail = pg_activity_table.get_html_string(attributes={
            'width': '60%',
            'align': 'left',
            'BORDERCOLOR': '#330000',
            'border': 2,
        })
    pg_activity_check_output = check_items_output(check_item, check_result, check_result_detail, rpt_format)
    return (check_item, check_result, pg_activity_check_output)

def pg_locks_check(dbconn, pg_version, rpt_format):
    check_item = 'Current Database Locks'
    check_result = 'OK'
    check_result_detail = ''
    if pg_version == 'pg9' or pg_version == 'hdw3':
        cursor = execSQL(dbconn, get_pg_locks_sql_pg9)
    if pg_version == 'pg8':
        cursor = execSQL(dbconn, get_pg_locks_sql_pg8)
    pg_locks_result = cursor.fetchall()
    column_names_list = [row[0] for row in cursor.description]
    pg_locks_table = PrettyTable(column_names_list)
    if cursor.rowcount > 0:
        check_result = 'NOT OK'
        for row in pg_locks_result:
            pg_locks_table.add_row(row)
    if rpt_format == 'text': 
        check_result_detail = pg_locks_table.get_string()
    if rpt_format == 'html':
        check_result_detail = pg_locks_table.get_html_string(attributes={
            'width': '60%',
            'align': 'left',
            'BORDERCOLOR': '#330000',
            'border': 2,
        })
    pg_locks_check_output = check_items_output(check_item, check_result, check_result_detail, rpt_format)
    return (check_item, check_result, pg_locks_check_output)

def table_bloat_check(db_list, rpt_format):
    check_item = 'Significant Bloat Heap Tables'
    check_result = 'OK'
    check_result_detail = ''
    for db in db_list:
        dbconn = pgdb.connect(database=db, host='localhost:5432', user='gpadmin')
        cursor = execSQL(dbconn, get_bloat_sql)
        bloat_result = cursor.fetchall()
        column_names_list = [row[0] for row in cursor.description]
        bloat_table = PrettyTable(column_names_list)
        if cursor.rowcount > 0:
            check_result = 'NOT OK'
            for row in bloat_result:
                bloat_table.add_row(row)
        if rpt_format == 'text': 
            check_result_detail += '\nDatabase: ' + db + '\n' + bloat_table.get_string() + '\n'
        if rpt_format == 'html':
            check_result_detail += '<div style="clear:both"><br><b><li>Database: ' + db + '</li></b><div style="clear:both">\n' + bloat_table.get_html_string(attributes={
            'width': '60%',
            'align': 'left',
            'BORDERCOLOR': '#330000',
            'border': 2,
        }) + '\n<br>'
        dbconn.close()
    bloat_table_check_output = check_items_output(check_item, check_result, check_result_detail, rpt_format)
    return (check_item, check_result, bloat_table_check_output)

def ao_bloat_check(db_list, rpt_format):
    check_item = 'Significant Bloat AO Tables'
    check_result = 'OK'
    check_result_detail = ''
    for db in db_list:
        dbconn = pgdb.connect(database=db, host='localhost:5432', user='gpadmin')
        cursor = execSQL(dbconn, get_ao_bloat_sql)
        bloat_result = cursor.fetchall()
        column_names_list = [row[0] for row in cursor.description]
        bloat_table = PrettyTable(column_names_list)
        if cursor.rowcount > 0:
            check_result = 'NOT OK'
            for row in bloat_result:
                bloat_table.add_row(row)
        if rpt_format == 'text': 
            check_result_detail += '\nDatabase: ' + db + '\n' + bloat_table.get_string() + '\n'
        if rpt_format == 'html':
            check_result_detail += '<div style="clear:both"><br><b><li>Database: ' + db + '</li></b><div style="clear:both">\n' + bloat_table.get_html_string(attributes={
            'width': '60%',
            'align': 'left',
            'BORDERCOLOR': '#330000',
            'border': 2,
        }) + '\n<br>'
        dbconn.close()
    bloat_table_check_output = check_items_output(check_item, check_result, check_result_detail, rpt_format)
    return (check_item, check_result, bloat_table_check_output)

def db_age_check(dbconn, rpt_format):
    check_item = 'Database Age'
    check_result = 'OK'
    check_result_detail = ''
    cursor = execSQL(dbconn, get_db_age_sql)
    db_age_result = cursor.fetchall()
    column_names_list = [row[0] for row in cursor.description]
    db_age_table = PrettyTable(column_names_list)
    for row in db_age_result:
        db_age_table.add_row(row)
        if row[-1] != 'BELOW WARN LIMIT':
            check_result = 'NOT OK'
    if rpt_format == 'text': 
        check_result_detail = db_age_table.get_string()
    if rpt_format == 'html':
        check_result_detail = db_age_table.get_html_string(attributes={
            'width': '60%',
            'align': 'left',
            'BORDERCOLOR': '#330000',
            'border': 2,
        })
    db_age_check_output = check_items_output(check_item, check_result, check_result_detail, rpt_format)
    return (check_item, check_result, db_age_check_output)

def table_age_check(db_list, rpt_format):
    check_item = 'Tables Age'
    check_result = 'OK'
    check_result_detail = ''
    for db in db_list:
        dbconn = pgdb.connect(database=db, host='localhost:5432', user='gpadmin')
        cursor = execSQL(dbconn, get_table_age_sql)
        table_age_result = cursor.fetchall()
        column_names_list = [row[0] for row in cursor.description]
        table_age_table = PrettyTable(column_names_list)
        for row in table_age_result:
            table_age_table.add_row(row)
            if row[-1] != 'BELOW WARN LIMIT':
                check_result = 'NOT OK'
        if rpt_format == 'text': 
            check_result_detail += '\nDatabase: ' + db + '\n' + table_age_table.get_string() + '\n'
        if rpt_format == 'html':
            check_result_detail += '<div style="clear:both"><br><b><li>Database: ' + db + '</li></b><div style="clear:both">\n' + table_age_table.get_html_string(attributes={
            'width': '60%',
            'align': 'left',
            'BORDERCOLOR': '#330000',
            'border': 2,
        }) + '\n<br>'
        dbconn.close()
    table_age_check_output = check_items_output(check_item, check_result, check_result_detail, rpt_format)
    return (check_item, check_result, table_age_check_output)

def temp_schema_check(db_list,rpt_format):
    check_item = 'Temp Schema'
    check_result = 'OK'
    check_result_detail = ''
    for db in db_list:
        dbconn = pgdb.connect(database=db, host='localhost:5432', user='gpadmin')
        cursor = execSQL(dbconn, get_temp_schema_sql)
        temp_schema_result = cursor.fetchall()
        column_names_list = [row[0] for row in cursor.description]
        temp_schema_table = PrettyTable(column_names_list)
        if cursor.rowcount > 0:
            check_result = 'NOT OK'
            for row in temp_schema_result:
                temp_schema_table.add_row(row)
        if rpt_format == 'text': 
            check_result_detail += '\nDatabase: ' + db + '\n' + temp_schema_table.get_string() + '\n'
        if rpt_format == 'html':
            check_result_detail += '<div style="clear:both"><br><b><li>Database: ' + db + '</li></b><div style="clear:both">\n' + temp_schema_table.get_html_string(attributes={
            'width': '60%',
            'align': 'left',
            'BORDERCOLOR': '#330000',
            'border': 2,
        }) + '\n<br>'
        dbconn.close()
    temp_schema_check_output = check_items_output(check_item, check_result, check_result_detail, rpt_format)
    return (check_item, check_result, temp_schema_check_output)

def stale_stats_check(db_list,rpt_format):
    check_item = 'Tables Statistics'
    check_result = 'OK'
    check_result_detail = ''
    for db in db_list:
        dbconn = pgdb.connect(database=db, host='localhost:5432', user='gpadmin')
        cursor = execSQL(dbconn, get_stale_stats_sql)
        stale_stats_result = cursor.fetchall()
        column_names_list = [row[0] for row in cursor.description]
        stale_stats_table = PrettyTable(column_names_list)
        if cursor.rowcount > 0:
            check_result = 'NOT OK'
            for row in stale_stats_result:
                stale_stats_table.add_row(row)
        if rpt_format == 'text': 
            check_result_detail += '\nDatabase: ' + db + '\n' + stale_stats_table.get_string() + '\n'
        if rpt_format == 'html':
            check_result_detail += '<div style="clear:both"><br><b><li>Database: ' + db + '</li></b><div style="clear:both">\n' + stale_stats_table.get_html_string(attributes={
            'width': '60%',
            'align': 'left',
            'BORDERCOLOR': '#330000',
            'border': 2,
        }) + '\n<br>'
        dbconn.close()
    stale_stats_check_output = check_items_output(check_item, check_result, check_result_detail, rpt_format)
    return (check_item, check_result, stale_stats_check_output)

def master_log_check(dbconn, rpt_format):
    check_item = 'Database Log'
    check_result = 'OK'
    check_result_detail = ''
    cursor = execSQL(dbconn, get_master_log_sql)
    master_log_result = cursor.fetchall()
    column_names_list = [row[0] for row in cursor.description]
    master_log_table = PrettyTable(column_names_list)
    if cursor.rowcount > 0:
        check_result = 'NOT OK'
        for row in master_log_result:
            master_log_table.add_row(row)
    if rpt_format == 'text': 
        check_result_detail = master_log_table.get_string()
    if rpt_format == 'html':
        check_result_detail = master_log_table.get_html_string(attributes={
            'width': '60%',
            'align': 'left',
            'BORDERCOLOR': '#330000',
            'border': 2,
        })
    master_log_check_output = check_items_output(check_item, check_result, check_result_detail, rpt_format)
    return (check_item, check_result, master_log_check_output)

##################  Main function ################## 
def hdw_health_check(configs):
    #### Connect DB and get hosts list in cluster
    rpt_format = configs['report_format']
    dbconn = pgdb.connect(database='postgres', host='localhost:5432', user='gpadmin')
    hosts_list = get_hosts_list(dbconn)
    db_list = get_db_list(dbconn)  
    pg_version = get_pg_version(dbconn)

    #### Start checks
    report_output_list = []
    if configs['db_version_check']['enabled']:
        get_db_version_output = get_db_version(dbconn,rpt_format)
        report_output_list.append(get_db_version_output)
    if configs['seg_config_check']['enabled']:
        seg_config_check_output = seg_config_check(dbconn,rpt_format)
        report_output_list.append(seg_config_check_output)
    if configs['os_version_check']['enabled']:
        os_version_check_output = os_version_check(hosts_list,rpt_format)
        report_output_list.append(os_version_check_output)
    if configs['cpu_cores_check']['enabled']:
        cpu_cores_check_output = cpu_cores_check(hosts_list,rpt_format)
        report_output_list.append(cpu_cores_check_output)
    if configs['memory_size_check']['enabled']:
        memory_size_check_output = memory_size_check(hosts_list,rpt_format)
        report_output_list.append(memory_size_check_output)
    if configs['diskspace_check']['enabled']:
        diskspace_check_output = diskspace_check(dbconn,rpt_format)
        report_output_list.append(diskspace_check_output)
    if configs['host_load_check']['enabled']:
        host_load_check_output = host_load_check(hosts_list,rpt_format)
        report_output_list.append(host_load_check_output)
    if configs['segments_status_check']['enabled'] and pg_version != 'hdw3':
        segments_check_output = segments_check(rpt_format)
        report_output_list.append(segments_check_output)
    if configs['standby_status_check']['enabled'] and pg_version != 'hdw3':
        standby_check_output = standby_check(dbconn, pg_version,rpt_format)
        report_output_list.append(standby_check_output)
    if configs['guc_check']['enabled']:
        guc_check_output = guc_check(dbconn,rpt_format)
        report_output_list.append(guc_check_output)
    if configs['res_queue_check']['enabled']:
        resqueue_check_output = resqueue_check(dbconn,rpt_format)
        report_output_list.append(resqueue_check_output)
    if configs['pg_activity_check']['enabled']:
        pg_activity_check_output = pg_activity_check(dbconn, pg_version,rpt_format)
        report_output_list.append(pg_activity_check_output)
    if configs['pg_locks_check']['enabled']:
        pg_locks_check_output = pg_locks_check(dbconn, pg_version,rpt_format)
        report_output_list.append(pg_locks_check_output)
    if configs['db_size_check']['enabled']:
        db_size_check_output = db_size_check(dbconn,rpt_format)
        report_output_list.append(db_size_check_output)
    if configs['schema_size_check']['enabled']:
        schema_size_check_output = schema_size_check(db_list,rpt_format)
        report_output_list.append(schema_size_check_output)
    if configs['table_size_check']['enabled']:
        table_size_check_output = table_size_check(db_list,rpt_format)
        report_output_list.append(table_size_check_output)
    if configs['heap_table_bloat_check']['enabled'] and pg_version != 'hdw3':
        heap_table_bloat_check_output = table_bloat_check(db_list,rpt_format)
        report_output_list.append(heap_table_bloat_check_output)
    if configs['ao_table_bloat_check']['enabled']:
        ao_table_bloat_check_output = ao_bloat_check(db_list, rpt_format)
        report_output_list.append(ao_table_bloat_check_output)
    if configs['data_skew_check']['enabled']:
        data_skew_check_output = data_skew_check(db_list,pg_version,rpt_format)
        report_output_list.append(data_skew_check_output)
    if configs['stale_stats_check']['enabled']:
        stale_stats_check_output = stale_stats_check(db_list,rpt_format)
        report_output_list.append(stale_stats_check_output)
    if configs['db_age_check']['enabled'] and pg_version != 'hdw3':
        db_age_check_output = db_age_check(dbconn,rpt_format)
        report_output_list.append(db_age_check_output)
    if configs['table_age_check']['enabled'] and pg_version != 'hdw3':
        table_age_check_output = table_age_check(db_list,rpt_format)
        report_output_list.append(table_age_check_output)
    if configs['temp_schema_check']['enabled']:
        temp_schema_check_output = temp_schema_check(db_list,rpt_format)
        report_output_list.append(temp_schema_check_output)
    if configs['master_log_check']['enabled']:
        master_log_check_output = master_log_check(dbconn,rpt_format)
        report_output_list.append(master_log_check_output)
    dbconn.close()

    #### Construct Report
    report_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())

    if rpt_format == 'text':
        report_header = (
            '# HashData Health Check Report\n'
            'Report Date: %s\n\n'
        ) % (report_time)
        check_summary_output = '## Database Check Summary\n'
        check_items_output='\n\n## Database Health Check Details\n'
        report_end = '\n'
    if rpt_format == 'html':
        report_header = """
            <html>
                <head>
                    <title>
                        HashData Health Check Report
                    </title>
                </head>
                <body>
                    <H1>HashData Health Check Report</H1>
                    <p><b>Report Date:<b> %s</p>
        """ % (report_time)
        check_summary_output = '<h2>Database Check Summary</h2>'
        check_items_output='''
            <div style="clear:both"><br><h2>Database Health Check Details</h2>
        '''
        report_end = """
                </body>
            </html>
        """

    check_summary_table = PrettyTable(["No.","Check Item","Check Result"])
    green_print_flag = '\033[1;32m'
    red_print_flag = '\033[1;31m'
    color_print_end_flag = '\033[0m'
    color_flag = ''
    for idx, item in enumerate(report_output_list):
        check_item = item[0]
        check_result = item[1]
        check_details = item[2]
        if rpt_format == 'text':
            if check_result == 'OK':
                color_flag = green_print_flag
            if check_result == 'NOT OK':
                color_flag = red_print_flag
            check_summary_table.add_row([color_flag+str(idx+1)+color_print_end_flag, color_flag+check_item+color_print_end_flag, color_flag+check_result+color_print_end_flag])
        if rpt_format == 'html':
            check_summary_table.add_row([idx+1,check_item,check_result])
        check_items_output += check_details
    if rpt_format == 'text':
        check_summary_output += check_summary_table.get_string()
    if rpt_format == 'html':
        check_summary_table_html = check_summary_table.get_html_string(attributes={
            'width': '60%',
            'align': 'left',
            'BORDERCOLOR': '#330000',
            'border': 2,
        })
        check_summary_table_html = re.sub('<td>%s</td>'%('NOT OK'), '<td bgcolor="%s">%s</td>'%('yellow', 'NOT OK'), check_summary_table_html)
        check_summary_output += check_summary_table_html
    report_output = report_header + check_summary_output + check_items_output + report_end
    
    #### Output report to file
    report_path = configs['report_path']
    if not os.path.exists(report_path):
        os.mkdir(report_path)
    report_suffix = '.rpt'
    if rpt_format == 'html':
        report_suffix = '.html'
    report_file = report_path + '/hdw-health-check-' + time.strftime("%Y-%m-%d", time.localtime()) + report_suffix
    report_output_without_color_flag  = re.sub(r'\033\[.*m','',report_output)
    f = codecs.open(report_file, 'w', 'utf-8')
    f.write(report_output_without_color_flag)
    f.close()
    if rpt_format == 'text':
        print(report_output)
    print('Health check report has been saved in %s' % report_file)

def main():
    parser = argparse.ArgumentParser(description='Run health check as defined in a YAML formatted control file.')
    parser.add_argument("-f", "--config-file", type=argparse.FileType('r'), metavar="<filename>", dest="file", help='A YAML file that contains the health check items setting.')
    args = parser.parse_args()
    if not args.file:
        parser.print_usage()
        return sys.exit(1)
    with args.file as f:
        try:
            configs = yaml.safe_load(f.read())
        except Exception as e:
            print(args.file.name + ' is not a valid YAML config file.\n')
            parser.print_usage()
            return sys.exit(1)
    hdw_health_check(configs)
     
if __name__ == "__main__":
    main()
