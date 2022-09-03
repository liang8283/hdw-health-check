# hdw-health-check

This tool can be used for health check on HashData 2x and Greenplum 5x/6x databases.

## Setup

### Prerequisites
1. A running HashData 2x or Greenplum 5x/6x database with `gpadmin` access.
2. `root` access or an OS user with `pip` permission on `master` node.
3. Passwordless `ssh` between master node and segment nodes for `gpadmin` user.

### Download and Install
1. Download `hdw_health_check.py` and `config.yml` in this repo.
2. Put above 2 files on master node and grant `execute` permission against `hdw_health_check.py`.

```
chmod +x hdw_health_check.py
```
3. Install following python library by the user who has `pip` permission.

- If master node has internet access, run following command to install the libary.

```
pip install prettytable
```

- If master node does not have internet access, download the `prettytable.tar.gz` in this repo and upload to master. 

```
tar -xzf prettytable.tar.gz
cd prettytable
pip install prettytable-1.0.1-py2.py3-none-any.whl
```
**Note**: The `whl` files in the above tarball is for CentOS 7.

## Run the health check

1. (Optional) Update the `config.yml` file. 

- **report_format**: `text` or `html`. The `text` format report is printed to the stdout and be saved to `hdw-health-check-YYYY-MM-DD.rpt` as well. The `html` format report is only saved to `hdw-health-check-YYYY-MM-DD.html`.
- **rreport_path**: Set the path where the report will be generated to. By default, the report will be created at `/home/gpadmin`.
- **enabled**: Set `true` or `false` to enable or disable a specific check item. By default, all items in the config file will be checked.

2. Run the health check using `gpadmin`.

```
python ./hdw_health_check.py -f config.yml
```

## Supported Check Items

| Check Item  | Supported Version | Description | 
|:------------|:------------|:------------|
|db_version_check|hdw2,hdw3,GP5,GP6|Check database version |
|seg_config_check|hdw2,hdw3,GP5,GP6| Get `gp_segment_configuration`|
|os_version_check|hdw2,hdw3,GP5,GP6|Check OS version for each host in cluster|
|cpu_cores_check|hdw2,hdw3,GP5,GP6| Check CPU cores for each host in cluster|
|memory_size_check|hdw2,hdw3,GP5,GP6| Check RAM size for each host in cluster|
|diskspace_check|hdw2,hdw3,GP5,GP6| Check free diskspace for database data directory|
|host_load_check|hdw2,hdw3,GP5,GP6| Get `uptime` output for each host|
|segments_status_check|hdw2,GP5,GP6|Check if there is any segments down. |
|standby_status_check|hdw2,GP5,GP6|Check if the standby master is sync or not. |
|guc_check|hdw2,hdw3,GP5,GP6|Get current important GUCs setting|
|res_queue_check|hdw2,hdw3,GP5,GP6|Get resource queue setting. If no resource queue other than `pg_default` exists, check result shows `NOT OK`.|
|db_size_check|hdw2,hdw3,GP5,GP6|Get db size for all databases in cluster|
|schema_size_check|hdw2,hdw3,GP5,GP6|Get all schemas size in each database|
|table_size_check|hdw2,hdw3,GP5,GP6|Get top 10 size tables in each database. **Note**: It could take some time to perform this check if the database is large.|
|data_skew_check| hdw2,hdw3,GP5,GP6|- For GP/HashData 2x, check table data skew by comparing the files size on OS across each segment. If the table size > 1GB and gap between max size and min size segment > 20%, the check result will be `NOT OK`. - For HashData 3x, check the coefficients for AO tables with rowcounts > 100,000.|
|heap_table_bloat_check| hdw2,GP5,GP6|Get the table list with (actual pages/expected page > 5).|
|ao_table_bloat_check|  hdw2,hdw3,GP5,GP6|Get the AO/AOCS table list with (total_tupcount/hidden_tupcount > 5).|
|db_age_check| hdw2,GP5,GP6|Check db age for each database across all segments. The result will be `NOT OK` if the age reaches the warn limit `2^31-1 - xid_stop_limit`.|
|table_age_check| hdw2,GP5,GP6|Get top 3 age tables from each segment and show `NOT OK` if the table age reaches the warn limit `2^31-1 - xid_stop_limit`.|
|temp_schema_check| hdw2,hdw3,GP5,GP6|Check master and all segments for any temp schemas existing.|
|pg_activity_check|hdw2,hdw3,GP5,GP6|Check current running queries in database. The check result will be `NOT OK` if any query runs > 1hr.|
|pg_locks_check| hdw2,hdw3,GP5,GP6|Check if there is any session holding the lock > 10mins.|
|stale_stats_check|hdw2,hdw3,GP5,GP6|Get a list of tables which have not been analyzed for > 7 days in each database.|
|master_log_check|hdw2,hdw3,GP5,GP6|Get the latest 100 PANIC or FATAL errors from pg_log|
